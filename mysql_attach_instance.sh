#!/bin/bash
# 将已有 MySQL 数据目录绑定为新的 systemd 服务
# 用法：sudo ./mysql_attach_instance.sh /path/to/existing/datadir [service-name]
# 如果未提供路径则进入向导模式

set -euo pipefail

GREEN='\033[1;32m' NC='\033[0m' CYAN='\033[1;36m' YELLOW='\E[1;33m' RED='\033[1;31m'
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
RES_DIR="$SCRIPT_DIR/RES"
VALIDATION_LIB="$RES_DIR/mysql_validation_functions.sh"
LOG_DIR="/var/log/mysql_attach_instance"
LOG_FILE="$LOG_DIR/mysql_attach_instance_$(date +%Y%m%d_%H%M%S).log"

# 审计日志函数（同时写入标准输出和审计文件）
audit() {
    local msg="$*"
    local ts=$(date '+%Y-%m-%d %H:%M:%S')
    >&2 echo "[$ts] $msg"
    sudo mkdir -p "$LOG_DIR" >/dev/null 2>&1 || true
    echo "[$ts] $msg" | sudo tee -a "$LOG_FILE" >/dev/null || true
}

log() {
    local level="$1"; shift
    local ts=$(date '+%Y-%m-%d %H:%M:%S')
    echo -e "[$ts] [$level] $*"
}

usage() {
    cat <<EOF
用法: sudo $0 /path/to/data_dir [service_name]
如果不提供参数，将交互询问。脚本会：
 - 验证数据目录
 - 生成 /etc/my.cnf.d/mysql-<name>.cnf（备份冲突）
 - 生成 /etc/systemd/system/<service>.service（备份冲突）
 - 设置权限 chown -R mysql:mysql
 - 尝试设置 SELinux 上下文
 - 生成客户端 /etc/my.cnf.d/mysql-<name>-client.cnf
 - daemon-reload, enable, start
EOF
}

# 检查必需命令
require_cmds=(systemctl rsync ss awk sed grep realpath)
for cmd in "${require_cmds[@]}"; do
    if ! command -v "$cmd" &>/dev/null; then
        log "WARN" "缺少命令: $cmd （某些功能可能受限）"
    fi
done

# 辅助: 规范化路径（保证 realpath 不存在时仍能返回原始输入）
normalize_path() { realpath -m "$1" 2>/dev/null || echo "$1"; }

# 载入验证库（如果存在）
if [[ -f "$VALIDATION_LIB" ]]; then
    # 该文件应仅定义函数，不应有副作用
    source "$VALIDATION_LIB" || log "WARN" "加载验证库失败"
fi

# 验证 queries 文件的哈希（防止被篡改）
HASH_FILE="$RES_DIR/mysql_validation_queries.sha256"
SKIP_VALIDATION_DUE_TO_HASH_MISMATCH=0
if [[ -f "$RES_DIR/mysql_validation_queries.sql" ]]; then
    # 计算当前哈希（可读校验）
    if command -v sha256sum &>/dev/null; then
        current_hash=$(sha256sum "$RES_DIR/mysql_validation_queries.sql" | awk '{print $1}') || current_hash=""
    else
        current_hash=""
    fi

    if [[ -f "$HASH_FILE" ]]; then
        stored_hash=$(cat "$HASH_FILE" 2>/dev/null || echo "")
        if [[ -n "$stored_hash" && -n "$current_hash" && "$stored_hash" != "$current_hash" ]]; then
            echo -e "\n${YELLOW}警告：验证查询文件校验值不匹配。${NC}"
            echo "  存储哈希: $stored_hash"
            echo "  当前哈希: $current_hash"
            read -p "是否退出脚本以检查文件？(yes 退出 / 回车 继续): " _choice_hash
            if [[ "$_choice_hash" =~ ^(yes|y|Y)$ ]]; then
                log "ERROR" "用户选择退出：查询文件哈希不匹配"
                exit 1
            else
                SKIP_VALIDATION_DUE_TO_HASH_MISMATCH=1
                audit "WARN: 查询文件哈希不匹配，继续但将禁用自动/交互验证。请运行 RES/deploy_validation_files.sh 修复哈希。"
            fi
        fi
    else
        echo -e "\n${YELLOW}警告：未找到哈希文件 $HASH_FILE 。${NC}"
        read -p "是否退出脚本以创建/检查哈希？(yes 退出 / 回车 继续): " _choice_hash2
        if [[ "$_choice_hash2" =~ ^(yes|y|Y)$ ]]; then
            log "ERROR" "用户选择退出：缺少查询文件哈希"
            exit 1
        else
            SKIP_VALIDATION_DUE_TO_HASH_MISMATCH=1
            audit "WARN: 缺少哈希文件，继续但将禁用自动/交互验证。请运行 RES/deploy_validation_files.sh 创建哈希。"
        fi
    fi
fi
# 辅助: 根据基础名称生成安全服务名
safe_name_from_dir() {
    local dir="$1"
    local base=$(basename "$dir")
    base=$(echo "$base" | sed 's/[^a-zA-Z0-9._-]/_/g')
    echo "$base"
}

# 查找一个未被占用的 TCP 端口（从 3306 起）
find_free_port() {
    local start=${1:-3306}
    for p in $(seq $start 65535); do
        if ! ss -tln 2>/dev/null | awk '{print $4}' | grep -q ":$p$"; then
            echo $p
            return 0
        fi
    done
    return 1
}

# 列出系统用户与组及其对应关系
show_system_users_groups() {
    echo "系统用户 (user:uid:gid:home:shell) 的部分列表："
    awk -F: '{print $1":"$3":"$4":"$6":"$7}' /etc/passwd | head -n 200 | sed 's/^/  /'
    echo ""
    echo "系统组 (group:gid:members)："
    awk -F: '{print $1":"$3":"$4}' /etc/group | head -n 200 | sed 's/^/  /'
    echo ""
}

# 创建系统用户（可选创建组并加入 mysql 组）
create_system_user() {
    local username="$1"
    local groupname="$2" # 可选
    audit "尝试创建用户: $username (组: ${groupname:-<default>})"

    # 如果组指定且不存在，先创建组
    if [[ -n "$groupname" ]]; then
        if ! getent group "$groupname" >/dev/null; then
            audit "创建组: $groupname"
            sudo groupadd "$groupname"
        fi
    fi

    # 创建系统用户（无登录，系统用户）
    if id "$username" &>/dev/null; then
        audit "用户已存在: $username"
        return 0
    fi

    local useradd_opts=( -r -M -s /sbin/nologin )
    if [[ -n "$groupname" ]]; then
        useradd_opts+=( -g "$groupname" )
    fi

    sudo useradd "${useradd_opts[@]}" "$username" || {
        audit "ERROR: useradd 创建用户失败: $username"
        return 1
    }

    audit "已创建用户: $username"

    # 如系统存在 mysql 组，推荐加入
    if getent group mysql >/dev/null; then
        read -p "是否将用户 $username 加入到 mysql 组? (yes/no): " add_mysql_grp
        if [[ "$add_mysql_grp" =~ ^(yes|y|Y)$ ]]; then
            sudo usermod -aG mysql "$username" || audit "WARN: usermod 添加到 mysql 组失败"
            audit "已将 $username 添加到 mysql 组"
        fi
    fi

    return 0
}

# 展示现有用户列表（user:group）并让用户通过序号选择或创建新用户
present_owner_choices() {
    local default_owner="$1"
    # 构建用户列表：user:group（primary group）
    mapfile -t USERS < <(awk -F: '{print $1}' /etc/passwd)
    declare -a ENTRIES
    for u in "${USERS[@]}"; do
        gid=$(getent passwd "$u" | cut -d: -f4 2>/dev/null || echo "")
        gname=$(getent group "$gid" | cut -d: -f1 2>/dev/null || echo "")
        if [[ -z "$gname" ]]; then
            gname="$(id -gn "$u" 2>/dev/null || echo "")"
        fi
        ENTRIES+=("$u:$gname")
    done
    # 将菜单输出到 stderr（使得当函数被命令替换捕获时，菜单仍显示在终端）
    >&2 echo "现有用户与主组："
    local i=1
    for e in "${ENTRIES[@]}"; do
        >&2 echo "  $i) $e"
        i=$((i+1))
    done
    >&2 echo "  $i) 使用默认属主: $default_owner"
    local create_index=$i
    i=$((i+1))
    >&2 echo "  $i) 创建新用户并进入创建向导"

    local choice
    while true; do
        >&2 printf "选择序号 (1-%s): " "$i"
        read choice
        if ! [[ "$choice" =~ ^[0-9]+$ ]]; then
            >&2 echo "请输入数字序号"
            continue
        fi
        if [[ $choice -ge 1 && $choice -le ${#ENTRIES[@]} ]]; then
            selected="${ENTRIES[$((choice-1))]}"
            echo "$selected"
            return 0
        elif [[ $choice -eq $create_index ]]; then
            # default
            echo "$default_owner"
            return 0
        elif [[ $choice -eq $((create_index+1)) ]]; then
            # create new user wizard
            >&2 printf "输入要创建的新用户名: "
            read newu
            >&2 printf "输入新用户主组 (留空则使用用户名作为组): "
            read newg
            if [[ -z "$newg" ]]; then
                newg=""
            fi
            if create_system_user "$newu" "$newg"; then
                # return created user:group
                if [[ -n "$newg" ]]; then
                    echo "$newu:$newg"
                else
                    # get primary group
                    ng=$(getent passwd "$newu" | cut -d: -f4)
                    gname=$(getent group "$ng" | cut -d: -f1 2>/dev/null || echo "$newu")
                    echo "$newu:$gname"
                fi
                return 0
            else
                >&2 echo "创建用户失败，重新选择。"
            fi
        else
            >&2 echo "序号超出范围，请重新输入。"
        fi
    done
}

# 主要操作：创建配置和 systemd unit
attach_instance() {
    local datadir="$1"
    local svc_name_base="$2"

    datadir=$(normalize_path "$datadir")

    if [[ ! -d "$datadir" ]]; then
        log "ERROR" "数据目录不存在: $datadir"
        return 1
    fi

    # 简单检测数据目录是否看起来像 MySQL
    if [[ ! -e "$datadir/ibdata1" && ! -d "$datadir/mysql" && ! -e "$datadir/mysqld.pid" ]]; then
        log "WARN" "目录内没有明显的 MySQL 文件（如 ibdata1 或 mysql 系统库）。请确认: $datadir"
        read -p "仍要继续并尝试创建服务吗? (yes/no): " yn
        if [[ ! "$yn" =~ ^(yes|y|Y)$ ]]; then
            log "INFO" "取消"
            return 1
        fi
    fi

    # service 名称
    local safe_base="$svc_name_base"
    if [[ -z "$safe_base" ]]; then
        safe_base=$(safe_name_from_dir "$datadir")
    fi
    local service_unit="mysql-${safe_base}.service"

    # 避免与已有服务冲突
    if systemctl list-unit-files --type=service | grep -q "^${service_unit}"; then
        local stamp=$(date +%s)
        service_unit="mysql-${safe_base}-${stamp}.service"
        log "INFO" "检测到同名服务，使用替代服务名: $service_unit"
    fi

    # 提供默认值并询问用户是否自定义关键参数
    local default_socket="${datadir%/}/mysqld.sock"
    local default_pid="${datadir%/}/mysqld.pid"
    local default_mysqlx_socket="${datadir%/}/mysqlx.sock"
    local default_mysqlx_port="$(find_free_port 33160 || echo 0)"
    local default_port="$(find_free_port 3306 || echo 0)"
    local default_owner="mysql"

    echo "检测到默认参数："
    echo "  服务单元: $service_unit"
    echo "  TCP 端口: ${default_port:-(未检测到)}"
    echo "  mysqlx 端口: ${default_mysqlx_port:-(未检测到)}"
    echo "  socket: $default_socket"
    echo "  pid-file: $default_pid"
    echo "  目录属主: $default_owner"

    read -p "使用上述默认值？(yes 使用 / no 自定义): " _use_def
    if [[ "$_use_def" =~ ^(no|n|N)$ ]]; then
        read -p "服务短名（不带前缀 mysql-，回车保持 $safe_base）: " input_base
        if [[ -n "$input_base" ]]; then
            safe_base="$input_base"
            service_unit="mysql-${safe_base}.service"
        fi
        read -p "TCP 端口 [${default_port}]: " input_port
        if [[ -n "$input_port" ]]; then default_port="$input_port"; fi
        read -p "mysqlx 端口 [${default_mysqlx_port}]: " input_mx
        if [[ -n "$input_mx" ]]; then default_mysqlx_port="$input_mx"; fi
        read -p "socket 文件路径 [${default_socket}]: " input_sock
        if [[ -n "$input_sock" ]]; then default_socket="$input_sock"; fi
        read -p "pid 文件路径 [${default_pid}]: " input_pid
        if [[ -n "$input_pid" ]]; then default_pid="$input_pid"; fi
            # 列出现有用户并以序号选择或创建新用户向导
            selected_owner=$(present_owner_choices "$default_owner")
            # present_owner_choices 返回 user:group 或 user 或 default
            if [[ "$selected_owner" == *":"* ]]; then
                default_owner="$selected_owner"
            else
                # 如果只返回用户名，尝试读取其主组
                if getent passwd "$selected_owner" >/dev/null; then
                    gid=$(getent passwd "$selected_owner" | cut -d: -f4)
                    gname=$(getent group "$gid" | cut -d: -f1 2>/dev/null || echo "")
                    if [[ -n "$gname" ]]; then
                        default_owner="$selected_owner:$gname"
                    else
                        default_owner="$selected_owner"
                    fi
                else
                    default_owner="$selected_owner"
                fi
            fi
    fi

    # 将最终值赋回变量
    local socket_path="$default_socket"
    local pid_file="$default_pid"
    local mysqlx_socket="$default_mysqlx_socket"
    local mysqlx_port="$default_mysqlx_port"
    local port="$default_port"
    local owner="$default_owner"

    # 派生 user 和 group，确保 group 存在；如果没有指定 group，则尝试获取用户的主组，或使用 mysql 组，或创建与用户名同名的组
    local owner_user="${owner%%:*}"
    local owner_group=""
    if [[ "$owner" == *":"* ]]; then
        owner_group="${owner#*:}"
    else
        # 尝试通过 passwd 获取主组名
        if getent passwd "$owner_user" >/dev/null; then
            local gid=$(getent passwd "$owner_user" | cut -d: -f4)
            owner_group=$(getent group "$gid" | cut -d: -f1)
        fi
    fi

    if [[ -z "$owner_group" ]]; then
        if getent group mysql >/dev/null; then
            owner_group="mysql"
        else
            owner_group="$owner_user"
        fi
    fi

    # 确保组存在
    if ! getent group "$owner_group" >/dev/null; then
        audit "创建组: $owner_group"
        sudo groupadd "$owner_group" || audit "WARN: 创建组 $owner_group 失败"
    fi

    audit "使用属主: ${owner_user}:${owner_group}"

    # 如果系统存在 mysql 组，建议使用 mysql 组作为实例属组（便于与系统 mysql 用户组一致）
    if getent group mysql >/dev/null; then
        if [[ "$owner_group" != "mysql" ]]; then
            read -p "系统存在 'mysql' 组。是否将实例属组改为 'mysql'？(yes/默认 yes): " use_mysql_grp
            if [[ -z "$use_mysql_grp" || "$use_mysql_grp" =~ ^(yes|y|Y)$ ]]; then
                owner_group="mysql"
                audit "已选择使用 mysql 组 作为属组"
            fi
        fi
    fi

    # 更新 owner 字符串为确定的 user:group
    owner="${owner_user}:${owner_group}"

    audit "最终参数: service_unit=$service_unit, port=$port, mysqlx_port=$mysqlx_port, socket=$socket_path, pid=$pid_file, owner=$owner, datadir=$datadir"

    # 生成配置文件路径
    local conf_dir="/etc/my.cnf.d"
    local conf_file="$conf_dir/mysql-${safe_base}.cnf"
    local client_conf_file="$conf_dir/mysql-${safe_base}-client.cnf"

    sudo mkdir -p "$conf_dir"

    # 备份已有文件
    if [[ -f "$conf_file" ]]; then
        sudo cp -a "$conf_file" "${conf_file}.backup.$(date +%Y%m%d_%H%M%S)" || true
        log "INFO" "已备份已存在的配置: $conf_file"
    fi
    if [[ -f "$client_conf_file" ]]; then
        sudo cp -a "$client_conf_file" "${client_conf_file}.backup.$(date +%Y%m%d_%H%M%S)" || true
        log "INFO" "已备份已存在的客户端配置: $client_conf_file"
    fi

# (已记录 datadir)
    # 写入服务专用 my.cnf（将 socket/pid/mysqlx 放在 datadir）
    log "INFO" "正在创建服务配置: $conf_file"
    sudo tee "$conf_file" > /dev/null <<EOF
# 自动生成: MySQL 实例配置
# 数据目录: $datadir
[mysqld]
    user = ${owner_user}
datadir = $datadir
socket = $socket_path
pid-file = $pid_file
skip-name-resolve = 1
EOF

    if [[ "$port" != "0" ]]; then
        sudo tee -a "$conf_file" > /dev/null <<EOF
port = $port
EOF
    fi

    # mysqlx 设置，若无法分配端口则尝试禁用或只设置 socket
    if [[ "$mysqlx_port" != "0" ]]; then
        sudo tee -a "$conf_file" > /dev/null <<EOF
mysqlx_socket = $mysqlx_socket
mysqlx_port = $mysqlx_port
mysqlx_bind_address = 127.0.0.1
EOF
    else
        sudo tee -a "$conf_file" > /dev/null <<EOF
# mysqlx disabled due to port allocation failure
mysqlx=OFF
EOF
    fi

    sudo tee -a "$conf_file" > /dev/null <<'EOF'
# 日志和其他项可按需添加
# log-error = /var/log/mysql/mysql-INSTANCE.err
EOF

    # 写入客户端配置（仅 socket，保证 mysql 命令可用）
    log "INFO" "正在创建客户端配置: $client_conf_file"
    sudo tee "$client_conf_file" > /dev/null <<EOF
# 客户端配置（实例: $safe_base）
[client]
socket = $socket_path

[mysql]
socket = $socket_path
EOF

    # systemd 单元文件
    local unit_path="/etc/systemd/system/$service_unit"
    if [[ -f "$unit_path" ]]; then
        sudo cp -a "$unit_path" "${unit_path}.backup.$(date +%Y%m%d_%H%M%S)" || true
    fi

[ -n "$unit_path" ] || true
    audit "正在创建 systemd 单元: $unit_path"
    sudo tee "$unit_path" > /dev/null <<EOF
[Unit]
Description=MySQL Instance - $safe_base
After=network.target

[Service]
Type=simple
# 使用派生的 user/group
User=${owner_user}
Group=${owner_group}
# Ensure datadir exists, set ownership and cleanup stale sockets before start
ExecStartPre=/bin/mkdir -p $datadir
ExecStartPre=/bin/chown -R ${owner_user}:${owner_group} $datadir
ExecStartPre=/bin/sh -c '/bin/rm -f "$socket_path" "${socket_path}.lock" "$mysqlx_socket" "${mysqlx_socket}.lock" || true'
# PIDFile and explicit socket/pid passed to mysqld
PIDFile=$pid_file
ExecStart=/usr/sbin/mysqld --defaults-file=$conf_file --pid-file=$pid_file --socket=$socket_path
LimitNOFILE=5000
Restart=on-failure
RestartSec=5s
TimeoutStartSec=120s

[Install]
WantedBy=multi-user.target
EOF

    # 权限设置：按用户选择设置属主与权限（默认 750）
    audit "设置目录属主为 ${owner} => $datadir"
    if id "${owner%%:*}" &>/dev/null; then
        sudo chown -R "${owner}" "$datadir" || audit "WARN: chown 失败"
    else
        audit "WARN: 指定属主用户不存在：${owner}，跳过 chown"
    fi
    sudo chmod 750 "$datadir" || true

    # 若 datadir 下存在残留 socket/lock，移除它们（避免启动竞争）
    sudo rm -f "${socket_path}" "${socket_path}.lock" "${mysqlx_socket}" "${mysqlx_socket}.lock" || true

    # 尝试设置 SELinux 上下文
    if command -v getenforce &>/dev/null && [[ "$(getenforce)" == "Enforcing" ]]; then
        log "INFO" "SELinux: Enforcing，尝试设置 fcontext 并 restorecon"
        if command -v semanage &>/dev/null; then
            sudo semanage fcontext -a -t mysqld_db_t "${datadir}(/.*)?" || true
            sudo restorecon -R "$datadir" || true
            log "INFO" "已使用 semanage/restorecon 设置 SELinux 上下文"
        else
            # fallback
            sudo chcon -R -t mysqld_db_t "$datadir" || true
            log "INFO" "使用 chcon 设置 SELinux 上下文（semanage 不可用）"
        fi
    else
        log "INFO" "SELinux 未启用或不可用，跳过上下文设置"
    fi

    # reload, enable, start
    log "INFO" "重新加载 systemd 配置"
    sudo systemctl daemon-reload

    log "INFO" "启用服务并尝试启动: $service_unit"
    sudo systemctl enable --now "$service_unit" || true

    # 等待短时间并检查状态
    sleep 2
    if sudo systemctl is-active --quiet "$service_unit"; then
        log "INFO" "服务启动成功: $service_unit"
        log "INFO" "Socket: $socket_path"
        if [[ "$port" != "0" ]]; then log "INFO" "Port: $port"; fi
        log "INFO" "配置文件: $conf_file"
        log "INFO" "systemd unit: $unit_path"
        echo
        echo -e "${GREEN}✓ 实例已附加并启动（如需查看日志：sudo journalctl -u $service_unit -n 200）${NC}"
        # 启动成功后，询问是否创建客户端快捷命令以便在本机直接连接
        read -p "是否为该实例创建客户端快捷命令 (e.g. /usr/local/bin/mysql-${safe_base})? (yes/no, 默认 yes): " _mkcli
        if [[ -z "$_mkcli" || "$_mkcli" =~ ^(yes|y|Y)$ ]]; then
            create_client_wrappers "$safe_base" "$socket_path" "$port"
        fi

        # 如果验证函数库存在，询问是否运行验证菜单（或因哈希不符而跳过）
        if declare -f init_validation_system >/dev/null 2>&1 && [[ -f "$RES_DIR/mysql_validation_queries.sql" ]]; then
            if [[ "${SKIP_VALIDATION_DUE_TO_HASH_MISMATCH:-0}" -eq 1 ]]; then
                audit "WARN: 验证已被禁用（查询文件哈希缺失或不匹配）。如需手动验证，请使用已创建的客户端命令执行查询。"
            else
                read -p "是否现在运行数据库验证（标准/详细/选择性）? (yes/no, 默认 no): " _runval
                if [[ "$_runval" =~ ^(yes|y|Y)$ ]]; then
                    # 初始化验证系统并进入验证菜单（显示失败原因并跳过验证而非静默失败）
                    if init_validation_system "$RES_DIR/mysql_validation_queries.sql"; then
                        audit "验证系统初始化成功: ${VALIDATION_QUERIES_FILE:-<unset>} ${VALIDATION_QUERIES_HASH:-<no-hash>}"
                        validation_menu "$socket_path" "$datadir" "$safe_base"
                    else
                        audit "ERROR: 验证系统初始化失败，跳过验证。请运行 RES/deploy_validation_files.sh 检查哈希或修复 ${RES_DIR}/ 下的文件。"
                    fi
                fi
            fi
        fi
        return 0
    else
        log "ERROR" "服务未能启动，请查看日志: sudo journalctl -u $service_unit --since '5 minutes ago'"
        return 1
    fi
}


# 创建客户端快捷命令（在 /usr/local/bin），便于使用 mysql-<name> 直接连接
create_client_wrappers() {
    local name="$1"
    local socket_path="$2"
    local port="$3"

    local mysql_bin=$(command -v mysql || echo "/usr/bin/mysql")
    local mysqladmin_bin=$(command -v mysqladmin || echo "/usr/bin/mysqladmin")
    local wrapper_dir="/usr/local/bin"
    sudo mkdir -p "$wrapper_dir"

    local wrapper="$wrapper_dir/mysql-$name"
    sudo tee "$wrapper" > /dev/null <<EOF
#!/bin/sh
if [ -S "$socket_path" ]; then
  exec $mysql_bin --socket="$socket_path" "\$@"
else
  exec $mysql_bin --protocol=TCP --port=$port --host=127.0.0.1 "\$@"
fi
EOF
    sudo chmod 755 "$wrapper"
    audit "已创建客户端快捷命令: $wrapper"

    local admin_wrapper="$wrapper_dir/mysqladmin-$name"
    sudo tee "$admin_wrapper" > /dev/null <<EOF
#!/bin/sh
if [ -S "$socket_path" ]; then
  exec $mysqladmin_bin --socket="$socket_path" "\$@"
else
  exec $mysqladmin_bin --protocol=TCP --port=$port --host=127.0.0.1 "\$@"
fi
EOF
    sudo chmod 755 "$admin_wrapper"
    audit "已创建 mysqladmin 快捷命令: $admin_wrapper"
}


# 验证菜单：调用 RES 中的验证函数库
validation_menu() {
    local socket_path="$1"
    local expected_datadir="$2"
    local instance_name="${3:-instance}"

    # helper: run a validation function and save full output to log file
    run_and_save_validation() {
        local label="$1"; shift
        local out_file="$LOG_DIR/validation_${instance_name}_${label}_$(date +%Y%m%d_%H%M%S).log"
        sudo mkdir -p "$LOG_DIR" >/dev/null 2>&1 || true
        echo "Validation report - $instance_name - $label" > "$out_file"
        echo "Generated: $(date)" >> "$out_file"
        echo "Socket: $socket_path" >> "$out_file"
        echo "Datadir: $expected_datadir" >> "$out_file"
        echo "========================================" >> "$out_file"

        # Execute the provided command (function name and args), capture both stdout/stderr
        local cmd_output
        # Temporarily disable nounset to protect against RES functions that assume unset vars
        set +u 2>/dev/null || true
        if cmd_output=$("$@" 2>&1); then
            local rc=0
        else
            local rc=$?
        fi
        set -u 2>/dev/null || true

        if [[ $rc -eq 0 ]]; then
            echo "$cmd_output" | tee -a "$out_file"
            audit "验证 ($label) 已保存: $out_file"
            # 如果是生成报告，尝试从输出中提取报告文件路径并提示
            if [[ "$label" == "report" || "$label" == "generate_report" ]]; then
                local rpt
                rpt=$(echo "$cmd_output" | grep -oE '/tmp/mysql_validation_report_[0-9_]+\.txt' | tail -1 || true)
                if [[ -n "$rpt" ]]; then
                    echo "生成的验证报告文件: $rpt"
                    audit "生成验证报告路径: $rpt"
                fi
            fi
            return 0
        else
            echo "$cmd_output" | tee -a "$out_file"
            audit "验证 ($label) 已保存（但执行失败）: $out_file"
            return $rc
        fi
    }

    # 将该实例的所有验证日志打包到归档，并尝试发布到 web 可见目录
    bundle_reports() {
        local inst="$1"
        sudo mkdir -p "$LOG_DIR/archives" >/dev/null 2>&1 || true
        local archive="$LOG_DIR/archives/validation_${inst}_bundle_$(date +%Y%m%d_%H%M%S).tar.gz"
        # 收集该实例的验证日志
        pushd "$LOG_DIR" >/dev/null 2>&1 || return 1
        tar -czf "$archive" validation_${inst}_*.log 2>/dev/null || {
            # 如果没有匹配的文件，创建空归档
            tar -czf "$archive" --files-from /dev/null
        }
        popd >/dev/null 2>&1 || true
        audit "已打包验证日志: $archive"

        # 尝试发布到 /var/www/html/mysql_attach_archives
        local webdir="/var/www/html/mysql_attach_archives"
        if [[ -d /var/www/html && -w /var/www/html || $(id -u) -eq 0 ]]; then
            sudo mkdir -p "$webdir" >/dev/null 2>&1 || true
            sudo cp -f "$archive" "$webdir/" || true
            sudo chmod 644 "$webdir/$(basename "$archive")" || true
            # 生成 URL 使用主机名或 IP
            host=$(hostname -f 2>/dev/null || hostname)
            echo "报告已发布: http://$host/mysql_attach_archives/$(basename "$archive")"
        else
            echo "归档文件位置: $archive"
            echo "若需从远程下载，可使用 scp:"
            echo "scp $(whoami)@$(hostname -I | awk '{print $1}'):$archive ./"
        fi
    }

    while true; do
        echo "\n请选择验证类型："
        echo "  1) 快速验证"
        echo "  2) 标准验证"
        echo "  3) 详细验证"
        echo "  4) 全面验证"
        echo "  5) 选择性验证（按需选择查询）"
        echo "  6) 生成验证报告"
        echo "  7) 退出验证菜单"
        read -p "选择(1-7): " vch
        case "$vch" in
            1) run_and_save_validation quick validate_database_quick_table "$socket_path" || true ;;
            2) run_and_save_validation standard validate_database_standard "$socket_path" "$expected_datadir" || true ;;
            3) run_and_save_validation detailed validate_database_detailed "$socket_path" "$expected_datadir" || true ;;
            4) run_and_save_validation comprehensive validate_database_extended "$socket_path" "$expected_datadir" || true ;;
            5) run_and_save_validation selective validate_database_selective "$socket_path" "$expected_datadir" || true ;;
            6) 
                # generate_validation_report may already write to stdout; capture and save
                run_and_save_validation report generate_validation_report "$socket_path" || true ;;
            7) break ;;
            *) echo "无效选择" ;;
        esac
    done
}

# 回滚 / 清理已创建的 unit 与配置
cleanup_instance() {
    local arg="$1"
    if [[ -z "$arg" ]]; then
        read -p "输入要清理的服务短名或 unit (例如 myddddd 或 mysql-myddd 或 mysql-myddd.service): " arg
    fi

    local unit_name="$arg"
    if [[ "$unit_name" != *.service ]]; then
        if [[ "$unit_name" == mysql-* ]]; then
            unit_name="${unit_name}.service"
        else
            unit_name="mysql-${unit_name}.service"
        fi
    fi

    local base="${unit_name#mysql-}"
    base="${base%.service}"

    local unit_path="/etc/systemd/system/$unit_name"
    local conf_file="/etc/my.cnf.d/mysql-${base}.cnf"
    local client_conf="/etc/my.cnf.d/mysql-${base}-client.cnf"

    echo "准备清理："
    echo "  service unit: $unit_path"
    echo "  instance config: $conf_file"
    echo "  client config: $client_conf"
    audit "开始清理: unit=$unit_name, conf=$conf_file, client_conf=$client_conf"
    read -p "确认清理并停止/禁用该服务？(yes/no): " conf
    if [[ ! "$conf" =~ ^(yes|y|Y)$ ]]; then
        echo "取消清理"
        return 1
    fi

    sudo systemctl stop "$unit_name" 2>/dev/null || true
    sudo systemctl disable "$unit_name" 2>/dev/null || true

    # 移除 unit 文件（并尝试恢复备份）
    if [[ -f "$unit_path" ]]; then
        sudo rm -f "$unit_path"
        sudo rm -f "/etc/systemd/system/${unit_name}.backup.*" 2>/dev/null || true
        audit "已移除 unit: $unit_path"
    fi

    # 恢复或移除配置文件
    if compgen -G "${conf_file}.backup.*" >/dev/null; then
        # 恢复最新备份
        local latest=$(ls -1t ${conf_file}.backup.* | head -1)
        sudo mv -f "$latest" "$conf_file"
        audit "已恢复配置文件备份: $latest -> $conf_file"
    else
        sudo rm -f "$conf_file" 2>/dev/null || true
        audit "已移除配置文件: $conf_file"
    fi

    if compgen -G "${client_conf}.backup.*" >/dev/null; then
        local latestc=$(ls -1t ${client_conf}.backup.* | head -1)
        sudo mv -f "$latestc" "$client_conf"
        audit "已恢复客户端配置备份: $latestc -> $client_conf"
    else
        sudo rm -f "$client_conf" 2>/dev/null || true
        audit "已移除客户端配置: $client_conf"
    fi

    sudo systemctl daemon-reload || true

    # 尝试清理 datadir 下可能的 socket/pid（但不删除数据目录）
    if [[ -f "$conf_file" ]]; then
        local old_datadir=$(grep -E "^\s*datadir\s*=" "$conf_file" 2>/dev/null | tail -1 | awk -F= '{print $2}' | xargs || true)
        if [[ -n "$old_datadir" && -d "$old_datadir" ]]; then
            sudo rm -f "${old_datadir%/}/mysqld.sock" "${old_datadir%/}/mysqld.pid" "${old_datadir%/}/mysqlx.sock" 2>/dev/null || true
            audit "已清理 $old_datadir 下的 socket/pid 文件"
        fi
    fi

    audit "清理完成: unit=$unit_name"
    echo "清理完成。如需手动删除数据目录请确保已备份后再进行操作。"
    return 0
}

# 在参数解析前支持 cleanup/rollback
if [[ $# -ge 1 && ( "$1" == "cleanup" || "$1" == "rollback" ) ]]; then
    # $2 可选，指定服务或短名
    cleanup_instance "${2:-}" || exit 1
    exit 0
fi

# 主体
if [[ $(id -u) -ne 0 ]]; then
    log "ERROR" "请以 root 或 sudo 运行此脚本"
    usage
    exit 1
fi

if [[ $# -ge 1 && "$1" == "-h" ]]; then
    usage; exit 0
fi

# 解析参数
INPUT_DIR=""
SERVICE_BASE=""
if [[ $# -ge 1 ]]; then
    INPUT_DIR="$1"
fi
if [[ $# -ge 2 ]]; then
    SERVICE_BASE="$2"
fi

if [[ -z "$INPUT_DIR" ]]; then
    echo "交互模式: 请输入已存在的 MySQL 数据目录（例如 /var/lib/mysql_copy）:"
    read -p "数据目录: " INPUT_DIR
fi

if [[ -z "$INPUT_DIR" ]]; then
    log "ERROR" "未提供数据目录"
    usage
    exit 1
fi

# 调用主流程
if attach_instance "$INPUT_DIR" "$SERVICE_BASE"; then
    log "INFO" "操作完成"
    exit 0
else
    log "ERROR" "操作失败"
    exit 2
fi
