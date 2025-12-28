#!/bin/bash
###############################################################################################
# MySQL 实例迁移脚本 
# 用法: sudo ./mysql_migrate.sh yes
# author: Deepseek
# test  : saint
###############################################################################################
# 目录结构：
        # .
        # ├── 提示词.txt
        # ├── mysql_migrate_V1.7.sh
        # ├── README.TXT
        # └── RES
        #     ├── check_sql_syntax.sh
        #     ├── deploy_validation_files.sh
        #     ├── mysql_validation_functions.sh
        #     └── mysql_validation_queries.sql

        # 2 directories, 7 files
###############################################################################################
# 说明：
# 目前仅仅在UOS 20 Server 环境下测试了默认实例的迁移。主要功能已经实现。
# 程序目前有很多问题需要进一步处理，但我懒。
###############################################################################################

set -euo pipefail

GREEN='\033[1;32m' green='\033[0;32m'  WHITE='\e[1;37m'   NC='\033[0m' # No Color       
RED='\033[1;31m'   red='\033[0;31m'    YELLOW='\E[1;33m'  yellow='\E[0;33m'    
BLUE='\E[1;34m'    blue='\E[0;34m'     PINK='\E[1;35m'    pink='\E[0;35m'  
purple='\e[0;35m'  PURPLE='\e[1;35m'   cyan='\e[0;36'     CYAN='\e[1;36m'

# 获取脚本所在目录
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SCRIPT_NAME="$(basename "${BASH_SOURCE[0]}")"

# 定义资源目录
RES_DIR="$SCRIPT_DIR/RES"

# 验证文件路径
VALIDATION_LIB="$RES_DIR/mysql_validation_functions.sh"
VALIDATION_QUERIES="$RES_DIR/mysql_validation_queries.sql"
DEPLOY_SCRIPT="$RES_DIR/deploy_validation_files.sh"

# 全局变量
declare -A MYSQL_INSTANCES
declare -a INSTANCE_NAMES
declare -A MYSQL_INSTANCES_SOURCE  # 存储配置来源信息
declare -A MYSQL_INSTANCES_CONF_FILES  # 存储相关的配置文件
declare -A MYSQL_INSTANCES_CONF_TIMESTAMP  # 配置文件时间戳
declare -A MYSQL_INSTANCES_START_TIME  # 启动时间
declare -A MYSQL_INSTANCES_RELIABILITY  # 配置可靠性评分

# 配置来源常量
CONF_SOURCE_SYSTEMD="systemd"
CONF_SOURCE_PROCESS="process"
CONF_SOURCE_CNF="cnf"
CONF_SOURCE_DEFAULT="default"
CONF_SOURCE_INFERRED="inferred"
CURRENT_INSTANCE=""
NEW_DATA_DIR=""
best_dir=""
instance_count=0
MIGRATION_LOG="/tmp/mysql_migrate_$(date +%Y%m%d_%H%M%S).log"

# 兼容变量（从 V1.9 回填到 V1.7 的初始化）
# PROBE 模式标志
PROBE_MODE=false
PROBE_JSON=false
LIST_ONLY=false

# provenance 存储（避免污染 MYSQL_INSTANCES 键）
declare -A MYSQL_INSTANCES_PROV


# 输出一行等号，宽度与终端宽度相同
echo_quote(){
    local term_width=$(tput cols 2>/dev/null || echo 80)
    printf "%${term_width}s" | tr ' ' '='
}

# 日志函数
log() {
    local level="$1"
    shift
    local message="$*"
    local timestamp=$(date '+%Y-%m-%d %H:%M:%S')
    
    case $level in
        "INFO") color=$GREEN ;;
        "WARN") color=$YELLOW ;;
        "ERROR") color=$RED ;;
        "DEBUG") color=$BLUE ;;
        *) color=$NC ;;
    esac
    
    # 判断是否为调试信息
    if [[ "$level" == "DEBUG" ]]; then
        # 调试信息输出到标准错误
        echo "[$timestamp] [$level] $message" >&2
    else
        # 其他信息输出到标准输出
        echo -e "${color}[$timestamp] [$level] $message${NC}"
    fi
    echo "[$timestamp] [$level] $message" >> "$MIGRATION_LOG"
}

# 检查资源文件
check_resource_files() {
    echo_quote
    echo -e "${CYAN}检查资源文件...${NC}"
    echo "脚本目录: $SCRIPT_DIR"
    echo "资源目录: $RES_DIR"
    echo_quote
    
    # 检查 RES 目录是否存在
    if [[ ! -d "$RES_DIR" ]]; then
        log "ERROR" "资源目录不存在: $RES_DIR"
        log "INFO" "请创建 RES 目录并放入以下文件："
        log "INFO" "  • mysql_validation_functions.sh"
        log "INFO" "  • mysql_validation_queries.sql"
        log "INFO" "  • deploy_validation_files.sh (可选)"
        return 1
    fi
    
    # 必需文件列表
    local required_files=(
        "mysql_validation_functions.sh"
        "mysql_validation_queries.sql"
    )
    
    local missing_files=()
    
    for file in "${required_files[@]}"; do
        local file_path="$RES_DIR/$file"
        if [[ ! -f "$file_path" ]]; then
            missing_files+=("$file")
            log "ERROR" "文件不存在: $file_path"
        else
            log "INFO" "找到文件: $file"
        fi
    done
    
    if [[ ${#missing_files[@]} -gt 0 ]]; then
        log "ERROR" "缺少必需文件: ${missing_files[*]}"
        log "INFO" "请确保所有文件都在 $RES_DIR/ 目录中"
        return 1
    fi
    
    # 显示文件信息
    echo_quote
    echo -e "${CYAN}资源文件详情：${NC}"
    for file in "${required_files[@]}"; do
        local file_path="$RES_DIR/$file"
        local size=$(du -h "$file_path" | cut -f1)
        local lines=$(wc -l < "$file_path")
        echo "  $file: $size, $lines 行"
    done
    
    # 检查 deploy 脚本（可选）
    if [[ -f "$DEPLOY_SCRIPT" ]]; then
        log "INFO" "找到部署脚本: $(basename "$DEPLOY_SCRIPT")"
    fi
    
    return 0
}

# 加载验证系统
load_validation_system() {
    log "INFO" "加载验证系统..."
    
    # 检查资源文件
    if ! check_resource_files; then
        return 1
    fi
    
    # 加载验证函数库
    if [[ ! -f "$VALIDATION_LIB" ]]; then
        log "ERROR" "验证函数库不存在: $VALIDATION_LIB"
        return 1
    fi
    
    # 导入验证函数
    source "$VALIDATION_LIB"
    
    # 初始化验证系统
    if ! init_validation_system "$VALIDATION_QUERIES"; then
        log "ERROR" "验证系统初始化失败"
        return 1
    fi
    
    log "INFO" "验证系统加载完成"
    log "INFO" "查询文件: $(basename "$VALIDATION_QUERIES")"
    log "INFO" "文件哈希: $VALIDATION_QUERIES_HASH"
    
    return 0
}

# 显示目录结构
show_directory_info() {
    echo_quote
    echo -e "${GREEN}目录结构信息：${NC}"
    echo_quote
    echo "主脚本: $SCRIPT_NAME"
    echo "所在目录: $SCRIPT_DIR"
    echo "资源目录: $RES_DIR"
    echo_quote
    
    if [[ -d "$RES_DIR" ]]; then
        echo "资源目录内容："
        ls -lh "$RES_DIR/" | tail -n +2 | sed 's/^/  /'
    fi
    
    echo_quote
}

# 显示脚本功能和警告
show_warning() {
    
    echo "================================================"
    echo "          MySQL 实例迁移脚本"
    echo "================================================"
    echo -e "${RED}警告：此脚本将进行数据库实例迁移操作${NC}"
    echo -e "${RED}请在非生产环境测试后再使用！${NC}"
    echo ""
    echo "主要功能："
    echo "1. 检测并列出所有 MySQL 实例"
    echo "2. 生成备份命令供自行备份过程参考"
    echo "3. 迁移 MySQL 数据目录到新位置"
    echo "4. 更新配置文件"
    echo ""
    echo "风险提示："
    echo "- 操作可能导致数据库服务中断"
    echo "- 数据丢失风险（请务必先自行备份）"
    echo "- 配置错误可能导致服务无法启动"
    echo ""
    echo "如需继续，请使用参数 'yes' 运行脚本："
    echo "  $0 yes"
    echo "================================================"
}

show_banner_ascii(){
    echo -e "${RED}

       ██████  ▄▄▄       ██▓ ███▄    █ ▄▄▄█████▓
     ▒██    ▒ ▒████▄    ▓██▒ ██ ▀█   █ ▓  ██▒ ▓▒
     ░ ▓██▄   ▒██  ▀█▄  ▒██▒▓██  ▀█ ██▒▒ ▓██░ ▒░
       ▒   ██▒░██▄▄▄▄██ ░██░▓██▒  ▐▌██▒░ ▓██▓ ░ 
     ▒██████▒▒ ▓█   ▓██▒░██░▒██░   ▓██░  ▒██▒ ░ 
     ▒ ▒▓▒ ▒ ░ ▒▒   ▓▒█░░▓  ░ ▒░   ▒ ▒   ▒ ░░   
     ░ ░▒  ░ ░  ▒   ▒▒ ░ ▒ ░░ ░░   ░ ▒░    ░    
     ░  ░  ░    ░   ▒    ▒ ░   ░   ░ ░   ░      
           ░        ░  ░ ░           ░                                                      
    ${NC}\r\n"
 
    # 输出一行等号，宽度与终端宽度相同
    #printf "%${term_width}s" | tr ' ' '='
    
    # 已经显示banner
    this_b_banner_shown=true

}

# 检查参数
# 检查参数
check_args() {
    local arg1="${1:-}"
    local arg2="${2:-}"
    
    if [[ -z "$arg1" ]]; then
        show_warning
        exit 1
    fi
    
    if [[ "$arg1" != "yes" ]]; then
        log "ERROR" "必须使用参数 'yes' 确认操作"
        show_warning
        exit 1
    fi

    if [[ "$arg2" == "list" ]]; then
        log "INFO" "仅列出 MySQL 实例，无其他操作"
        LIST_ONLY=true
    fi
}

# 确认用户知晓风险
confirm_risk() {
    #echo ""
    echo -e "${YELLOW}请仔细阅读以下警告：${NC}"
    echo "1. 此操作将导致选中的 MySQL 实例服务重启"
    echo "2. 迁移过程中数据库将不可用"
    echo "3. 虽然脚本会生成备份命令，但您必须手动执行备份"
    echo "4. 操作失败可能导致数据损坏"
    echo ""
    
    local response=""
    while [[ ! "$response" =~ ^(yes|YES|y|Y)$ ]]; do
        read -p "我已知晓风险并确认要继续 (yes/no): " response
        if [[ "$response" =~ ^(no|NO|n|N)$ ]]; then
            log "INFO" "用户取消操作"
            exit 0
        fi
    done
}

show_storage_info(){
        log "INFO" "文件系统及存储信息："
        echo_quote
        sudo lsblk
        echo_quote
        sudo lsblk -f
        echo_quote
        
        sudo vgs
        echo_quote
        sudo lvs
        echo_quote
        sudo pvs

        echo_quote
        sudo df -h
        echo_quote
        sudo df -ahT
        echo_quote
        
        sudo cat /etc/fstab
        echo_quote
}

get_directory_size_formatted() {
    local dir="$1"
    
    if [[ ! -d "$dir" ]]; then
        echo "0B"
        return 1
    fi
    
    # 首先尝试使用 du -h（最简单）
    if command -v du &>/dev/null; then
        local size_human=$(sudo du -sh "$dir" 2>/dev/null | cut -f1)
        if [[ -n "$size_human" ]]; then
            # 规范化输出格式（确保有单位）
            if [[ "$size_human" =~ ^[0-9]+$ ]]; then
                # 如果没有单位，假设是KB（du -h 通常不会这样）
                echo "${size_human}K"
            else
                echo "$size_human"
            fi
            return 0
        fi
    fi
    
    # 备用方法：计算并格式化
    echo "正在计算目录大小..." >&2
    
    local size_bytes=0
    
    # 方法1：使用 du -sb（最快）
    if command -v du &>/dev/null; then
        size_bytes=$(sudo du -sb "$dir" 2>/dev/null | awk '{print $1}')
    fi
    
    # 方法2：如果 du 失败，使用 find（慢但可靠）
    if [[ -z "$size_bytes" ]] || [[ ! "$size_bytes" =~ ^[0-9]+$ ]]; then
        size_bytes=$(sudo find "$dir" -type f -printf "%s\n" 2>/dev/null | awk '{sum+=$1} END {print sum+0}')
    fi
    
    # 方法3：最后的备用方案
    if [[ -z "$size_bytes" ]] || [[ ! "$size_bytes" =~ ^[0-9]+$ ]]; then
        size_bytes=$(sudo find "$dir" -type f -exec stat -c %s {} \; 2>/dev/null | awk '{sum+=$1} END {print sum+0}')
    fi
    
    # 格式化输出
    if [[ -z "$size_bytes" ]] || [[ "$size_bytes" -eq 0 ]]; then
        echo "0B"
        return 1
    fi
    
    # 格式化函数
    format_size() {
        local bytes=$1
        
        local units=("B" "K" "M" "G" "T")
        local unit_index=0
        
        while [[ $bytes -ge 1024 && $unit_index -lt 4 ]]; do
            bytes=$((bytes / 1024))
            unit_index=$((unit_index + 1))
        done
        
        echo "${bytes}${units[$unit_index]}"
    }
    
    format_size "$size_bytes"
    return 0
}

# 最终推荐使用的函数（别名）
get_dir_size() {
    get_directory_size_formatted "$@"
}

# comb.txt
# 包含 mysql_migrate_V1.9.sh 中 detect_mysql_instances 所需的仅在 V1.9 中出现的函数，按调用先后顺序排列

# 1) extract_opt_from_cmd: 从命令行/ExecStart 中提取 --key 或 --key=value
extract_opt_from_cmd() {
    local cmd="$1" key="$2" token next=0
    for token in $cmd; do
        if [[ "$token" == --${key}=* ]]; then
            echo "${token#--${key}=}" | tr -d '"' || true
            return 0
        fi
        if [[ "$token" == --${key} ]]; then
            next=1
            continue
        fi
        if [[ $next -eq 1 ]]; then
            echo "$token" | tr -d '"' || true
            return 0
        fi
    done
    return 1
}


# 2) parse_proc_cmd: 从 /proc/<pid>/cmdline 读取并调用 extract_opt_from_cmd
parse_proc_cmd() {
    local pid="$1" key="$2"
    local cmd
    if [[ -r "/proc/$pid/cmdline" ]]; then
        cmd=$(tr '\0' ' ' < /proc/$pid/cmdline || true)
        extract_opt_from_cmd "$cmd" "$key"
    fi
}


# 3) parse_unit_execstart: 从 systemd ExecStart（含 drop-in）中提取选项
parse_unit_execstart() {
    local svc="$1" key="$2"
    local exec
    # 首先获取 unit 的 ExecStart
    exec=$(systemctl show --property=ExecStart --value "$svc" 2>/dev/null || true)
    # 再读取可能的 drop-in 覆盖（/etc/systemd/system/<svc>.d/*.conf, /run/systemd/system/...）
    local dropin_exec=""
    get_systemd_dropin_execstart() {
        local s="$1" d f
        for d in "/etc/systemd/system/$s.d" "/run/systemd/system/$s.d"; do
            if [[ -d "$d" ]]; then
                for f in "$d"/*.conf; do
                    [[ -f "$f" ]] || continue
                    # 读取所有 ExecStart= 行
                    while IFS= read -r line; do
                        if [[ "$line" =~ ^ExecStart= ]]; then
                            # 去掉前缀 ExecStart=
                            dropin_exec+=" ${line#ExecStart=}"
                        fi
                    done < "$f"
                done
            fi
        done
    }
    get_systemd_dropin_execstart "$svc" || true
    # 如果 drop-in 存在，优先使用 drop-in 中的 ExecStart 列表
    if [[ -n "$dropin_exec" ]]; then
        exec="$dropin_exec $exec"
    fi
    if [[ -n "$exec" ]]; then
        extract_opt_from_cmd "$exec" "$key"
    fi
}


# 4) parse_cnf_value: 从 cnf 文件读取 key=value（更鲁棒的 awk 实现）
parse_cnf_value() {
    local file="$1" key="$2"
    if [[ ! -f "$file" ]]; then
        return 1
    fi
    awk -v k="$key" 'BEGIN{IGNORECASE=1} $0 ~ "^\\s*"k"\\s*="{sub(/^[ \t]*/,"",$0); sub(/\s*#.*$/,"",$0); split($0,a, "="); v=a[2]; for(i=3;i<=length(a);i++){v=v"="a[i]} gsub(/^[ \t]+|[ \t]+$/,"",v); val=v} END{if(val) print val}' "$file" | tail -n1
}


# 5) infer_datadir_from_lsof: 当常规方法失败时，使用 lsof 或 /proc/<pid>/fd 推断 datadir
infer_datadir_from_lsof() {
    local pid="$1"
    if [[ -z "$pid" || "$pid" -eq 0 ]]; then
        return 1
    fi
    if ! command -v lsof >/dev/null 2>&1; then
        return 1
    fi
    # 列出 PID 打开的文件，寻找常见 MySQL 文件名，返回其父目录
    local matches
    matches=$(lsof -p "$pid" -Fn 2>/dev/null | sed -n 's/^n//p' | egrep '/(ibdata1|ib_logfile|\.ibd$|mysql.sock|my\.cnf|aria\.|\.frm$)' | sed 's:/[^/]*$::' | sort | uniq)
    if [[ -n "$matches" ]]; then
        # 返回第一个匹配目录
        echo "$(echo "$matches" | head -n1)"
        return 0
    fi
    # 兜底尝试通过 /proc/$pid/fd 链接查找
    if [[ -d "/proc/$pid/fd" ]]; then
        local f p
        for f in /proc/$pid/fd/*; do
            p=$(readlink -f "$f" 2>/dev/null || true)
            if [[ "$p" =~ /(ibdata1|ib_logfile|\.ibd$|mysql.sock|\.frm$) ]]; then
                echo "$(dirname "$p")"
                return 0
            fi
        done
    fi
    return 1
}

# 分析配置来源并评分
analyze_configuration_sources() {
    local instance_name="$1"
    local service="${MYSQL_INSTANCES["${instance_name}_service"]}"
    local datadir="${MYSQL_INSTANCES["${instance_name}_datadir"]}"
    local socket="${MYSQL_INSTANCES["${instance_name}_socket"]}"
    local port="${MYSQL_INSTANCES["${instance_name}_port"]}"
    local pid="${MYSQL_INSTANCES["${instance_name}_pid"]}"
    
    local source_info=""
    local reliability_score=0
    local conf_files=()
    local conf_timestamps=()
    local MAX_SCORE=100  # 最大分数

    # 在每次加分后检查是否超过最大值
    # reliability_score=$((reliability_score + 30))
    # if [[ $reliability_score -gt $MAX_SCORE ]]; then
    #     reliability_score=$MAX_SCORE
    # fi
        
    # 使用关联数组去重
    declare -A unique_conf_files
    declare -A unique_conf_timestamps

    echo_quote
    echo -e "${CYAN}分析实例配置来源: $service${NC}"
    echo_quote
    
    # 1. 检查systemd服务配置
    if [[ "$service" != "mysqld.service" ]] && [[ "$service" != "未知" ]] && [[ ! "$service" =~ ^wild_mysqld_ ]]; then
        local service_file="${MYSQL_INSTANCES["${instance_name}_service_file_path"]:-}"
        
        if [[ -f "$service_file" ]]; then
            echo "✓ systemd服务配置: $service_file"
            echo "  修改时间: $(stat -c %y "$service_file" 2>/dev/null || echo '未知')"
            echo "  文件大小: $(stat -c %s "$service_file" 2>/dev/null || echo '未知') 字节"
            
            unique_conf_files["systemd:$service_file"]=1
            unique_conf_timestamps["systemd:$service_file"]="$(stat -c %Y "$service_file" 2>/dev/null || echo 0)"
            reliability_score=$((reliability_score + 25))
            [[ $reliability_score -gt $MAX_SCORE ]] && reliability_score=$MAX_SCORE
            
            # 检查systemd配置中是否有明确的参数
            if grep -q "datadir\|socket\|port" "$service_file" 2>/dev/null; then
                echo "  → 包含明确的MySQL参数配置"
                reliability_score=$((reliability_score + 5))
                [[ $reliability_score -gt $MAX_SCORE ]] && reliability_score=$MAX_SCORE
            fi
        else
            echo "✗ systemd配置文件不存在: $service_file"
        fi
    fi
    
    # 2. 检查进程信息（如果运行中）
    if [[ -n "$pid" ]] && [[ "$pid" -gt 0 ]]; then
        echo "✓ 运行中的进程: PID=$pid"
        
        # 获取进程启动时间
        local start_time=$(stat -c %Y "/proc/$pid" 2>/dev/null || echo 0)
        if [[ $start_time -gt 0 ]]; then
            local start_time_str=$(date -d "@$start_time" '+%Y-%m-%d %H:%M:%S' 2>/dev/null || echo "未知")
            echo "  启动时间: $start_time_str"
            MYSQL_INSTANCES_START_TIME["${instance_name}"]="$start_time_str"
            reliability_score=$((reliability_score + 20))
            [[ $reliability_score -gt $MAX_SCORE ]] && reliability_score=$MAX_SCORE
        fi
        
        # 获取进程命令行
        if [[ -r "/proc/$pid/cmdline" ]]; then
            local cmdline=$(tr '\0' ' ' < "/proc/$pid/cmdline" 2>/dev/null || true)
            echo "  命令行: $cmdline"
        fi
    fi
    
    # 3. 检查MySQL配置文件
    local cnf_files_found=()
    
    # 常见配置文件位置
    local common_cnf_paths=(
        "/etc/my.cnf"
        "/etc/mysql/my.cnf"
        "/etc/my.cnf.d/mysql-server.cnf"
        "/etc/my.cnf.d/$service.cnf"
        "/etc/mysql/conf.d/mysql.cnf"
        "/etc/mysql/mysql.conf.d/mysqld.cnf"
        "/etc/mysql/mariadb.conf.d/50-server.cnf"
        "$HOME/.my.cnf"
        "$datadir/my.cnf"
        "/usr/local/etc/my.cnf"
    )
    
    # 从进程获取--defaults-file
    if [[ -n "$pid" ]] && [[ "$pid" -gt 0 ]]; then
        local defaults_file=$(parse_proc_cmd "$pid" defaults-file 2>/dev/null || true)
        if [[ -n "$defaults_file" ]] && [[ -f "$defaults_file" ]]; then
            common_cnf_paths=("$defaults_file" "${common_cnf_paths[@]}")
            echo "✓ 进程指定配置文件: $defaults_file"
        fi
    fi
    
    # 从systemd获取--defaults-file
    if [[ -n "$service" ]] && [[ "$service" != "未知" ]]; then
        local defaults_file=$(parse_unit_execstart "$service" defaults-file 2>/dev/null || true)
        if [[ -n "$defaults_file" ]] && [[ -f "$defaults_file" ]]; then
            common_cnf_paths=("$defaults_file" "${common_cnf_paths[@]}")
            echo "✓ systemd指定配置文件: $defaults_file"
        fi
    fi
    
    # 检查所有配置文件
    for cnf_file in "${common_cnf_paths[@]}"; do
        if [[ -f "$cnf_file" ]]; then
            # 检查是否包含相关配置
            if grep -q "datadir\|socket\|port" "$cnf_file" 2>/dev/null; then
                echo "✓ MySQL配置文件: $cnf_file"
                echo "  修改时间: $(stat -c %y "$cnf_file" 2>/dev/null || echo '未知')"
                
                conf_files+=("cnf:$cnf_file")
                conf_timestamps+=("$(stat -c %Y "$cnf_file" 2>/dev/null || echo 0)")
                cnf_files_found+=("$cnf_file")
                reliability_score=$((reliability_score + 15))
                
                # 检查配置是否匹配当前实例
                local cnf_datadir=$(parse_cnf_value "$cnf_file" datadir 2>/dev/null || true)
                local cnf_socket=$(parse_cnf_value "$cnf_file" socket 2>/dev/null || true)
                local cnf_port=$(parse_cnf_value "$cnf_file" port 2>/dev/null || true)
                
                if [[ "$cnf_datadir" == "$datadir" ]]; then
                    echo "  → datadir匹配: $datadir"
                    reliability_score=$((reliability_score + 5))
                fi
                if [[ "$cnf_socket" == "$socket" ]]; then
                    echo "  → socket匹配: $socket"
                    reliability_score=$((reliability_score + 5))
                fi
            fi
        fi
    done
    
    # 4. 智能判断配置来源
    if [[ "$service" =~ ^wild_mysqld_ ]]; then
        source_info="$CONF_SOURCE_PROCESS"
    elif [[ -n "$pid" ]] && [[ "$pid" -gt 0 ]]; then
        # 检查是否有systemd配置文件
        local service_file="${MYSQL_INSTANCES["${instance_name}_service_file_path"]:-}"
        if [[ -f "$service_file" ]]; then
            # 检查是否通过systemd启动
            if ps -o cmd= -p "$pid" 2>/dev/null | grep -q "systemd"; then
                source_info="$CONF_SOURCE_SYSTEMD"
            else
                source_info="$CONF_SOURCE_PROCESS"
            fi
        else
            source_info="$CONF_SOURCE_PROCESS"
        fi
    elif [[ -f "${MYSQL_INSTANCES["${instance_name}_service_file_path"]:-}" ]]; then
        source_info="$CONF_SOURCE_SYSTEMD"
    else
        source_info="$CONF_SOURCE_INFERRED"
    fi
    
    # 5. 转换为数组存储
    local conf_files_array=("${!unique_conf_files[@]}")
    local conf_timestamps_array=()
    for key in "${!unique_conf_files[@]}"; do
        conf_timestamps_array+=("${unique_conf_timestamps[$key]}")
    done
    
    # 存储配置来源信息
    MYSQL_INSTANCES_SOURCE["${instance_name}"]="$source_info"
    MYSQL_INSTANCES_CONF_FILES["${instance_name}"]=$(IFS='|'; echo "${conf_files_array[*]}")
    MYSQL_INSTANCES_CONF_TIMESTAMP["${instance_name}"]=$(IFS='|'; echo "${conf_timestamps_array[*]}")
    MYSQL_INSTANCES_RELIABILITY["${instance_name}"]="$reliability_score"
    
    echo "配置来源: $source_info"
    echo "配置可靠性评分: $reliability_score/100"
    echo_quote
    
    return 0
}

# 分析配置来源并评分
analyze_configuration_sources_old() {
    local instance_name="$1"
    local service="${MYSQL_INSTANCES["${instance_name}_service"]}"
    local datadir="${MYSQL_INSTANCES["${instance_name}_datadir"]}"
    local socket="${MYSQL_INSTANCES["${instance_name}_socket"]}"
    local port="${MYSQL_INSTANCES["${instance_name}_port"]}"
    local pid="${MYSQL_INSTANCES["${instance_name}_pid"]}"
    
    local source_info=""
    local reliability_score=0
    local conf_files=()
    local conf_timestamps=()
    
    echo_quote
    echo -e "${CYAN}分析实例配置来源: $service${NC}"
    echo_quote
    
    # 1. 检查systemd服务配置
    if [[ "$service" != "mysqld.service" ]] && [[ "$service" != "未知" ]]; then
        local service_file="${MYSQL_INSTANCES["${instance_name}_service_file_path"]:-}"
        
        if [[ -f "$service_file" ]]; then
            echo "✓ systemd服务配置: $service_file"
            echo "  修改时间: $(stat -c %y "$service_file" 2>/dev/null || echo '未知')"
            echo "  文件大小: $(stat -c %s "$service_file" 2>/dev/null || echo '未知') 字节"
            
            conf_files+=("systemd:$service_file")
            conf_timestamps+=("$(stat -c %Y "$service_file" 2>/dev/null || echo 0)")
            reliability_score=$((reliability_score + 30))
            
            # 检查systemd配置中是否有明确的参数
            if grep -q "datadir\|socket\|port" "$service_file" 2>/dev/null; then
                echo "  → 包含明确的MySQL参数配置"
                reliability_score=$((reliability_score + 10))
            fi
        else
            echo "✗ systemd配置文件不存在: $service_file"
        fi
    fi
    
    # 2. 检查进程信息（如果运行中）
    if [[ -n "$pid" ]] && [[ "$pid" -gt 0 ]]; then
        echo "✓ 运行中的进程: PID=$pid"
        
        # 获取进程启动时间
        local start_time=$(stat -c %Y "/proc/$pid" 2>/dev/null || echo 0)
        if [[ $start_time -gt 0 ]]; then
            local start_time_str=$(date -d "@$start_time" '+%Y-%m-%d %H:%M:%S' 2>/dev/null || echo "未知")
            echo "  启动时间: $start_time_str"
            MYSQL_INSTANCES_START_TIME["${instance_name}"]="$start_time_str"
            reliability_score=$((reliability_score + 20))
        fi
        
        # 获取进程命令行
        if [[ -r "/proc/$pid/cmdline" ]]; then
            local cmdline=$(tr '\0' ' ' < "/proc/$pid/cmdline" 2>/dev/null || true)
            echo "  命令行: $cmdline"
            
            # 检查是否通过systemd启动
            if echo "$cmdline" | grep -q "systemd"; then
                echo "  → 通过systemd启动"
                source_info="$CONF_SOURCE_SYSTEMD"
            else
                echo "  → 直接启动"
                source_info="$CONF_SOURCE_PROCESS"
            fi
        fi
    fi
    
    # 3. 检查MySQL配置文件
    local cnf_files_found=()
    
    # 常见配置文件位置
    local common_cnf_paths=(
        "/etc/my.cnf"
        "/etc/mysql/my.cnf"
        "/etc/my.cnf.d/mysql-server.cnf"
        "/etc/my.cnf.d/$service.cnf"
        "/etc/mysql/conf.d/mysql.cnf"
        "/etc/mysql/mysql.conf.d/mysqld.cnf"
        "/etc/mysql/mariadb.conf.d/50-server.cnf"
        "$HOME/.my.cnf"
        "$datadir/my.cnf"
        "/usr/local/etc/my.cnf"
    )
    
    # 从进程获取--defaults-file
    if [[ -n "$pid" ]] && [[ "$pid" -gt 0 ]]; then
        local defaults_file=$(parse_proc_cmd "$pid" defaults-file 2>/dev/null || true)
        if [[ -n "$defaults_file" ]] && [[ -f "$defaults_file" ]]; then
            common_cnf_paths=("$defaults_file" "${common_cnf_paths[@]}")
            echo "✓ 进程指定配置文件: $defaults_file"
        fi
    fi
    
    # 从systemd获取--defaults-file
    if [[ -n "$service" ]] && [[ "$service" != "未知" ]]; then
        local defaults_file=$(parse_unit_execstart "$service" defaults-file 2>/dev/null || true)
        if [[ -n "$defaults_file" ]] && [[ -f "$defaults_file" ]]; then
            common_cnf_paths=("$defaults_file" "${common_cnf_paths[@]}")
            echo "✓ systemd指定配置文件: $defaults_file"
        fi
    fi
    
    # 检查所有配置文件
    for cnf_file in "${common_cnf_paths[@]}"; do
        if [[ -f "$cnf_file" ]]; then
            # 检查是否包含相关配置
            if grep -q "datadir\|socket\|port" "$cnf_file" 2>/dev/null; then
                echo "✓ MySQL配置文件: $cnf_file"
                echo "  修改时间: $(stat -c %y "$cnf_file" 2>/dev/null || echo '未知')"
                
                conf_files+=("cnf:$cnf_file")
                conf_timestamps+=("$(stat -c %Y "$cnf_file" 2>/dev/null || echo 0)")
                cnf_files_found+=("$cnf_file")
                reliability_score=$((reliability_score + 15))
                
                # 检查配置是否匹配当前实例
                local cnf_datadir=$(parse_cnf_value "$cnf_file" datadir 2>/dev/null || true)
                local cnf_socket=$(parse_cnf_value "$cnf_file" socket 2>/dev/null || true)
                local cnf_port=$(parse_cnf_value "$cnf_file" port 2>/dev/null || true)
                
                if [[ "$cnf_datadir" == "$datadir" ]]; then
                    echo "  → datadir匹配: $datadir"
                    reliability_score=$((reliability_score + 5))
                fi
                if [[ "$cnf_socket" == "$socket" ]]; then
                    echo "  → socket匹配: $socket"
                    reliability_score=$((reliability_score + 5))
                fi
            fi
        fi
    done
    
    # 4. 检查自定义启动脚本
    if [[ -n "$service" ]] && [[ "$service" != "未知" ]]; then
        # 检查服务是否使用自定义脚本
        local exec_start=$(systemctl show "$service" --property=ExecStart --value 2>/dev/null || true)
        
        if [[ "$exec_start" =~ \.sh$ ]] || [[ "$exec_start" =~ /usr/local/ ]]; then
            local script_path=$(echo "$exec_start" | awk '{print $1}')
            if [[ -f "$script_path" ]]; then
                echo "✓ 自定义启动脚本: $script_path"
                echo "  修改时间: $(stat -c %y "$script_path" 2>/dev/null || echo '未知')"
                
                conf_files+=("script:$script_path")
                conf_timestamps+=("$(stat -c %Y "$script_path" 2>/dev/null || echo 0)")
                reliability_score=$((reliability_score + 25))
            fi
        fi
    fi
    
    # 5. 检查是否"野生"实例（没有systemd配置）
    if [[ "$service" == "mysqld.service" ]] || [[ -z "$service" ]] || [[ "$service" == "未知" ]]; then
        if [[ -n "$pid" ]] && [[ "$pid" -gt 0 ]]; then
            echo "⚠ 疑似野生实例: 有进程但没有明确的systemd服务"
            source_info="$CONF_SOURCE_PROCESS"
            reliability_score=$((reliability_score - 10))
        else
            echo "⚠ 默认实例配置"
            source_info="$CONF_SOURCE_DEFAULT"
            reliability_score=$((reliability_score - 20))
        fi
    fi
    
    # 如果没有找到明确的来源，标记为推断
    if [[ -z "$source_info" ]]; then
        source_info="$CONF_SOURCE_INFERRED"
        echo "⚠ 配置来源为推断"
    fi
    
    # 存储配置来源信息
    MYSQL_INSTANCES_SOURCE["${instance_name}"]="$source_info"
    MYSQL_INSTANCES_CONF_FILES["${instance_name}"]=$(IFS='|'; echo "${conf_files[*]}")
    MYSQL_INSTANCES_CONF_TIMESTAMP["${instance_name}"]=$(IFS='|'; echo "${conf_timestamps[*]}")
    MYSQL_INSTANCES_RELIABILITY["${instance_name}"]="$reliability_score"
    
    echo "配置可靠性评分: $reliability_score/100"
    echo_quote
    
    return 0
}

# 检测野生MySQL实例（没有systemd服务的）
detect_wild_mysql_instances() {
    echo_quote
    echo -e "${CYAN}检测野生MySQL实例...${NC}"
    echo_quote
    
    # 1. 查找所有mysqld进程
    local mysql_pids=$(pgrep -x mysqld 2>/dev/null || ps aux | grep -E '[m]ysqld\b' | awk '{print $2}')
    local wild_instances=0
    
    for pid in $mysql_pids; do
        # 检查PID是否有效
        if [[ ! "$pid" =~ ^[0-9]+$ ]] || [[ "$pid" -le 1 ]]; then
            continue
        fi
        
        # 检查是否已经有对应的systemd服务
        local has_systemd_service=false
        for name in "${INSTANCE_NAMES[@]}"; do
            local instance_pid=${MYSQL_INSTANCES["${name}_pid"]}
            if [[ "$instance_pid" == "$pid" ]]; then
                has_systemd_service=true
                break
            fi
        done
        
        if [[ "$has_systemd_service" == false ]]; then
            echo "发现野生MySQL实例: PID=$pid"
            
            # 获取进程信息
            local cmdline=""
            if [[ -r "/proc/$pid/cmdline" ]]; then
                cmdline=$(tr '\0' ' ' < "/proc/$pid/cmdline" 2>/dev/null || echo "")
            fi
            
            # 提取配置参数
            local datadir=$(extract_opt_from_cmd "$cmdline" datadir 2>/dev/null || echo "")
            local socket=$(extract_opt_from_cmd "$cmdline" socket 2>/dev/null || echo "")
            local port=$(extract_opt_from_cmd "$cmdline" port 2>/dev/null || echo "")
            
            # 如果没有明确参数，尝试推断
            if [[ -z "$datadir" ]]; then
                datadir=$(infer_datadir_from_lsof "$pid" 2>/dev/null || echo "/var/lib/mysql")
            fi
            
            # 获取监听端口
            if [[ -z "$port" ]] && command -v ss &>/dev/null; then
                port=$(sudo ss -tlnp 2>/dev/null | grep "pid=$pid" | awk '{print $4}' | cut -d: -f2 | sort -nu | head -1 | tr '\n' ',' | sed 's/,$//' || echo "")
            fi
            
            # 创建野生实例记录
            wild_instances=$((wild_instances + 1))
            local instance_name="wild_instance_${wild_instances}"
            INSTANCE_NAMES+=("$instance_name")
            
            # 计算数据目录大小
            local dir_size=""
            if [[ -d "$datadir" ]]; then
                dir_size=$(get_dir_size "$datadir")
            fi
            
            # 存储野生实例信息
            MYSQL_INSTANCES["${instance_name}_service"]="wild_mysqld_$pid"
            MYSQL_INSTANCES["${instance_name}_service_file"]="无systemd配置"
            MYSQL_INSTANCES["${instance_name}_service_file_path"]=""
            MYSQL_INSTANCES["${instance_name}_size"]="$dir_size"
            MYSQL_INSTANCES["${instance_name}_datadir"]="$datadir"
            MYSQL_INSTANCES["${instance_name}_port"]="$port"
            MYSQL_INSTANCES["${instance_name}_socket"]="$socket"
            MYSQL_INSTANCES["${instance_name}_pid"]="$pid"
            MYSQL_INSTANCES["${instance_name}_status"]="active"
            MYSQL_INSTANCES["${instance_name}_pidfile"]=""
            
            echo "  数据目录: $datadir"
            echo "  大小: ${dir_size:-未知}"
            echo "  端口: ${port:-未知}"
            echo "  Socket: ${socket:-未知}"
            echo ""
            
            # 分析配置来源
            analyze_configuration_sources "$instance_name"
        fi
    done
    
    if [[ $wild_instances -eq 0 ]]; then
        echo "未发现野生MySQL实例"
    else
        echo "共发现 $wild_instances 个野生MySQL实例"
    fi
    
    echo_quote
    return 0
}

# 过滤和标记重复实例
filter_and_mark_duplicates() {
    echo_quote
    echo -e "${CYAN}检查重复数据目录...${NC}"
    echo_quote
    
    declare -A datadir_map  # 数据目录 -> 实例名称数组
    declare -A datadir_primary  # 数据目录 -> 主实例名称
    
    # 收集所有数据目录信息
    for name in "${INSTANCE_NAMES[@]}"; do
        local datadir=${MYSQL_INSTANCES["${name}_datadir"]}
        if [[ "$datadir" != "<unknown>" ]] && [[ -n "$datadir" ]]; then
            if [[ -z "${datadir_map[$datadir]+x}" ]]; then
                datadir_map["$datadir"]="$name"
            else
                datadir_map["$datadir"]="${datadir_map[$datadir]},$name"
            fi
        fi
    done
    
    # 找出重复的数据目录
    local duplicate_count=0
    for datadir in "${!datadir_map[@]}"; do
        IFS=',' read -ra instances <<< "${datadir_map[$datadir]}"
        
        if [[ ${#instances[@]} -gt 1 ]]; then
            duplicate_count=$((duplicate_count + 1))
            echo "发现重复数据目录: $datadir"
            echo "  关联实例:"
            
            # 选择主实例（最高可靠性的活跃实例）
            local primary_instance=""
            local highest_score=0
            
            for instance in "${instances[@]}"; do
                local service=${MYSQL_INSTANCES["${instance}_service"]}
                local status=${MYSQL_INSTANCES["${instance}_status"]}
                local reliability=${MYSQL_INSTANCES_RELIABILITY["${instance}"]:-0}
                local pid=${MYSQL_INSTANCES["${instance}_pid"]}
                
                echo "    - $service (状态: $status, 可靠性: $reliability)"
                
                # 选择策略：优先活跃且可靠性高的
                if [[ "$status" == "active" ]] && [[ -n "$pid" ]] && [[ "$pid" -gt 0 ]]; then
                    if [[ $reliability -gt $highest_score ]]; then
                        highest_score=$reliability
                        primary_instance="$instance"
                    fi
                fi
            done
            
            # 如果没有活跃实例，选择可靠性最高的
            if [[ -z "$primary_instance" ]]; then
                for instance in "${instances[@]}"; do
                    local reliability=${MYSQL_INSTANCES_RELIABILITY["${instance}"]:-0}
                    if [[ $reliability -gt $highest_score ]]; then
                        highest_score=$reliability
                        primary_instance="$instance"
                    fi
                done
            fi
            
            if [[ -n "$primary_instance" ]]; then
                datadir_primary["$datadir"]="$primary_instance"
                local primary_service=${MYSQL_INSTANCES["${primary_instance}_service"]}
                echo "  主实例: $primary_service"
                
                # 标记其他实例为重复
                for instance in "${instances[@]}"; do
                    if [[ "$instance" != "$primary_instance" ]]; then
                        MYSQL_INSTANCES["${instance}_is_duplicate"]="true"
                        MYSQL_INSTANCES["${instance}_duplicate_of"]="$primary_instance"
                    fi
                done
            fi
            
            echo ""
        fi
    done
    
    if [[ $duplicate_count -eq 0 ]]; then
        echo "未发现重复数据目录"
    else
        echo -e "${YELLOW}发现 $duplicate_count 个重复数据目录${NC}"
        echo -e "${RED}警告：多个实例共享同一数据目录可能导致数据损坏！${NC}"
    fi
    
    echo_quote
    return 0
}

detect_mysql_instances() {
    echo_quote
    log "INFO" "检测 MySQL 实例..."
    INSTANCE_NAMES=()
    instance_count=0

    mapfile -t services < <(systemctl list-unit-files --type=service 2>/dev/null | awk '{print $1}' | grep -E 'mysql|mariadb' | head -n 200)

    for svc in "${services[@]}"; do
        # 跳过模板单元
        if [[ "$svc" =~ @\.service$ ]]; then
            continue
        fi

        # 初始化
        local status pid fragment datadir socket port pidfile mysqlx_socket mysqlx_port defaults_file service_file_path
        local datadir_res socket_res port_res pidfile_res mysqlx_socket_res mysqlx_port_res
        local provenance_datadir provenance_socket provenance_port
        provenance_datadir=""
        provenance_socket=""
        provenance_port=""
        status=$(systemctl show --property=ActiveState --value "$svc" 2>/dev/null || echo "unknown")
        pid=$(systemctl show --property=MainPID --value "$svc" 2>/dev/null || echo "0")
        fragment=$(systemctl show --property=FragmentPath --value "$svc" 2>/dev/null || echo "<none>")

        datadir=""; socket=""; port=""; pidfile=""; mysqlx_socket=""; mysqlx_port=""

        if [[ -n "$pid" && "$pid" -gt 1 && -r "/proc/$pid/cmdline" ]]; then
            datadir=$(parse_proc_cmd "$pid" datadir || true)
            if [[ -n "$datadir" ]]; then provenance_datadir="proc:$pid"; fi
            socket=$(parse_proc_cmd "$pid" socket || true)
            if [[ -n "$socket" ]]; then provenance_socket="proc:$pid"; fi
            port=$(parse_proc_cmd "$pid" port || true)
            if [[ -n "$port" ]]; then provenance_port="proc:$pid"; fi
            pidfile=$(parse_proc_cmd "$pid" pid-file || true)
            mysqlx_socket=$(parse_proc_cmd "$pid" mysqlx_socket || true)
            mysqlx_port=$(parse_proc_cmd "$pid" mysqlx_port || true)
            defaults_file=$(parse_proc_cmd "$pid" defaults-file || true)
        fi

        if [[ -z "$datadir" ]]; then
            datadir=$(parse_unit_execstart "$svc" datadir || true)
            if [[ -n "$datadir" && -z "${provenance_datadir:-}" ]]; then provenance_datadir="exec:$svc"; fi
        fi
        if [[ -z "$socket" ]]; then
            socket=$(parse_unit_execstart "$svc" socket || true)
            if [[ -n "$socket" && -z "${provenance_socket:-}" ]]; then provenance_socket="exec:$svc"; fi
        fi
        if [[ -z "$port" ]]; then
            port=$(parse_unit_execstart "$svc" port || true)
            if [[ -n "$port" && -z "${provenance_port:-}" ]]; then provenance_port="exec:$svc"; fi
        fi
        if [[ -z "${defaults_file:-}" ]]; then
            defaults_file=$(parse_unit_execstart "$svc" defaults-file || true)
        fi

        if [[ -n "${defaults_file:-}" && -f "${defaults_file:-}" ]]; then
            datadir_res=$(parse_cnf_value "${defaults_file:-}" datadir || true)
            if [[ -n "$datadir_res" && -z "${datadir}" ]]; then datadir="$datadir_res"; provenance_datadir="defaults:${defaults_file}"; fi
            socket_res=$(parse_cnf_value "${defaults_file:-}" socket || true)
            if [[ -n "$socket_res" && -z "${socket}" ]]; then socket="$socket_res"; provenance_socket="defaults:${defaults_file}"; fi
            port_res=$(parse_cnf_value "${defaults_file:-}" port || true)
            if [[ -n "$port_res" && -z "${port}" ]]; then port="$port_res"; provenance_port="defaults:${defaults_file}"; fi
            pidfile_res=$(parse_cnf_value "${defaults_file:-}" pid-file || true)
            mysqlx_socket_res=$(parse_cnf_value "${defaults_file:-}" mysqlx_socket || true)
            mysqlx_port_res=$(parse_cnf_value "${defaults_file:-}" mysqlx_port || true)
            pidfile=${pidfile:-$pidfile_res}
            mysqlx_socket=${mysqlx_socket:-$mysqlx_socket_res}
            mysqlx_port=${mysqlx_port:-$mysqlx_port_res}
        fi

        # 兜底配置文件
        short=$(echo "$svc" | sed -E 's/^mysql-//; s/\.service$//')
        # 在使用 cnf 兜底前，尝试通过 lsof/proc fd 推断（针对 pid 存在但 cmdline/execstart 没有 datadir 的情况）
        if [[ -z "$datadir" && -n "$pid" && "$pid" -ne 0 ]]; then
            inferred=$(infer_datadir_from_lsof "$pid" || true)
            if [[ -n "$inferred" ]]; then
                datadir="$inferred"
                provenance_datadir="lsof:$pid"
            fi
        fi

        if [[ -z "$datadir" ]]; then
            for f in "/etc/my.cnf.d/mysql-${short}.cnf" "/etc/my.cnf"; do
                if [[ -f "$f" ]]; then v=$(parse_cnf_value "$f" datadir || true); if [[ -n "$v" ]]; then datadir="$v"; provenance_datadir="cnf:$f"; break; fi; fi
            done
        fi
        if [[ -z "$socket" ]]; then
            for f in "/etc/my.cnf.d/mysql-${short}.cnf" "/etc/my.cnf"; do
                if [[ -f "$f" ]]; then v=$(parse_cnf_value "$f" socket || true); if [[ -n "$v" ]]; then socket="$v"; provenance_socket="cnf:$f"; break; fi; fi
            done
        fi
        if [[ -z "$port" ]]; then
            for f in "/etc/my.cnf.d/mysql-${short}.cnf" "/etc/my.cnf"; do
                if [[ -f "$f" ]]; then v=$(parse_cnf_value "$f" port || true); if [[ -n "$v" ]]; then port="$v"; provenance_port="cnf:$f"; break; fi; fi
            done
        fi

        datadir=${datadir:-"<unknown>"}
        socket=${socket:-"<unknown>"}
        port=${port:-"<unknown>"}
        pidfile=${pidfile:-"<unknown>"}
        mysqlx_port=${mysqlx_port:-"<unknown>"}
        mysqlx_socket=${mysqlx_socket:-"<unknown>"}

        # 尺寸
        local dir_size=""
        if [[ -d "$datadir" ]]; then dir_size=$(get_dir_size "$datadir" || true); fi

        instance_count=$((instance_count+1))
        local instance_name="instance_${instance_count}"
        INSTANCE_NAMES+=("$instance_name")

        # 获取服务配置文件路径
        service_file_path="$fragment"
        if [[ "$fragment" == "<none>" ]] || [[ ! -f "$fragment" ]]; then
            # 尝试通过 systemctl cat 获取
            local cat_output=$(systemctl cat "$svc" 2>/dev/null)
            if [[ -n "$cat_output" ]]; then
                service_file_path=$(echo "$cat_output" | grep -E "^#" | grep "Loaded:" | awk -F'[\(;]' '{print $2}' | sed 's/^\s*//;s/\s*$//' | head -1)
            fi
            
            if [[ -z "$service_file_path" ]] || [[ ! -f "$service_file_path" ]]; then
                # 尝试常见位置
                local possible_paths=(
                    "/etc/systemd/system/$svc"
                    "/usr/lib/systemd/system/$svc"
                    "/lib/systemd/system/$svc"
                )
                
                for path in "${possible_paths[@]}"; do
                    if [[ -f "$path" ]]; then
                        service_file_path="$path"
                        break
                    fi
                done
            fi
        fi

        # 存储到实例信息中
        MYSQL_INSTANCES["${instance_name}_service_file_path"]="$service_file_path"
        MYSQL_INSTANCES["${instance_name}_service"]="$svc"
        MYSQL_INSTANCES["${instance_name}_service_file"]="$fragment"
        MYSQL_INSTANCES["${instance_name}_size"]="$dir_size"
        MYSQL_INSTANCES["${instance_name}_datadir"]="$datadir"
        MYSQL_INSTANCES["${instance_name}_port"]="$port"
        MYSQL_INSTANCES["${instance_name}_socket"]="$socket"
        MYSQL_INSTANCES["${instance_name}_pid"]="$pid"
        MYSQL_INSTANCES["${instance_name}_status"]="$status"
        MYSQL_INSTANCES["${instance_name}_pidfile"]="$pidfile"

        # 在存储实例信息后添加：
        analyze_configuration_sources "$instance_name"


        log "INFO" "发现实例: $svc (状态: $status, 端口: $port, 数据目录: $datadir, 大小: ${dir_size:-未知})"

        # 如果处于 probe 模式，构建 JSON 片段或打印 provenance（在最后统一输出）
        if [[ "$PROBE_MODE" == true ]]; then
            # 存储 provenance 字段
            MYSQL_INSTANCES_PROV["${instance_name}_datadir"]="${provenance_datadir:-unknown}"
            MYSQL_INSTANCES_PROV["${instance_name}_socket"]="${provenance_socket:-unknown}"
            MYSQL_INSTANCES_PROV["${instance_name}_port"]="${provenance_port:-unknown}"
        fi
    done

    if [[ $instance_count -eq 0 ]]; then
        instance_count=1
        local instance_name="instance_1"
        INSTANCE_NAMES+=("$instance_name")
        MYSQL_INSTANCES["${instance_name}_service"]="mysqld.service"
        MYSQL_INSTANCES["${instance_name}_service_file"]="未知"
        MYSQL_INSTANCES["${instance_name}_datadir"]="/var/lib/mysql"
        MYSQL_INSTANCES["${instance_name}_port"]="3306"
        MYSQL_INSTANCES["${instance_name}_socket"]="/var/lib/mysql/mysql.sock"
        MYSQL_INSTANCES["${instance_name}_pid"]=""
        MYSQL_INSTANCES["${instance_name}_status"]="unknown"
        MYSQL_INSTANCES["${instance_name}_pidfile"]="未知"
        log "WARN" "未发现明确实例，使用默认配置"
    fi

    log "INFO" "共发现 $instance_count 个 MySQL 实例"

    # 检测野生实例
    detect_wild_mysql_instances

    # 过滤重复实例
    filter_and_mark_duplicates

    这段保留，但注释掉，因为 V1.9 之后不再自动输出 JSON
    if [[ "$PROBE_MODE" == true ]]; then
        # 输出结构化 JSON
        local out="["
        local first=1
        for name in "${INSTANCE_NAMES[@]}"; do
            local svc=${MYSQL_INSTANCES["${name}_service"]}
            local file=${MYSQL_INSTANCES["${name}_service_file"]}
            local datadir=${MYSQL_INSTANCES["${name}_datadir"]}
            local port=${MYSQL_INSTANCES["${name}_port"]}
            local socket=${MYSQL_INSTANCES["${name}_socket"]}
            local pid=${MYSQL_INSTANCES["${name}_pid"]}
            local status=${MYSQL_INSTANCES["${name}_status"]}
            local size=${MYSQL_INSTANCES["${name}_size"]}
            local prov_datadir=${MYSQL_INSTANCES_PROV["${name}_datadir"]}
            local prov_socket=${MYSQL_INSTANCES_PROV["${name}_socket"]}
            local prov_port=${MYSQL_INSTANCES_PROV["${name}_port"]}
            if [[ $first -eq 1 ]]; then first=0; else out+=","; fi
            # 简单 JSON 字符串化（假设字段不包含复杂字符）
            out+="{\"service\":\"$svc\",\"service_file\":\"$file\",\"datadir\":\"$datadir\",\"datadir_prov\":\"$prov_datadir\",\"port\":\"$port\",\"port_prov\":\"$prov_port\",\"socket\":\"$socket\",\"socket_prov\":\"$prov_socket\",\"pid\":\"$pid\",\"status\":\"$status\",\"size\":\"$size\"}"
        done
        out+="]"
        if [[ "$PROBE_JSON" == true ]]; then
            echo "$out"
        else
            # 更可读的多行输出
            echo "$out" | python3 -c 'import sys,json; print(json.dumps(json.loads(sys.stdin.read()), indent=2, ensure_ascii=False))' 2>/dev/null || echo "$out"
        fi
        exit 0
    fi
}

# 改进的实例选择显示函数
select_instance_enhanced() {
    echo_quote
    echo -e "${GREEN}检测到的 MySQL 实例：${NC}"
    echo_quote
    
    # 按配置可靠性排序
    local sorted_indices=()
    for i in "${!INSTANCE_NAMES[@]}"; do
        sorted_indices+=("$i")
    done
    
    # 冒泡排序（简单实现）
    for ((i=0; i<${#sorted_indices[@]}-1; i++)); do
        for ((j=0; j<${#sorted_indices[@]}-i-1; j++)); do
            local idx1=${sorted_indices[$j]}
            local idx2=${sorted_indices[$((j+1))]}
            local name1=${INSTANCE_NAMES[$idx1]}
            local name2=${INSTANCE_NAMES[$idx2]}
            local score1=${MYSQL_INSTANCES_RELIABILITY["${name1}"]:-0}
            local score2=${MYSQL_INSTANCES_RELIABILITY["${name2}"]:-0}
            
            if [[ $score1 -lt $score2 ]]; then
                # 交换
                sorted_indices[$j]=$idx2
                sorted_indices[$((j+1))]=$idx1
            fi
        done
    done
    
    # 显示排序后的实例列表
    for order in "${!sorted_indices[@]}"; do
        local i=${sorted_indices[$order]}
        local instance=${INSTANCE_NAMES[$i]}
        local service=${MYSQL_INSTANCES["${instance}_service"]}
        local datadir=${MYSQL_INSTANCES["${instance}_datadir"]}
        local port=${MYSQL_INSTANCES["${instance}_port"]}
        local socket=${MYSQL_INSTANCES["${instance}_socket"]}
        local service_file=${MYSQL_INSTANCES["${instance}_service_file"]}
        local pid=${MYSQL_INSTANCES["${instance}_pid"]}
        local status=${MYSQL_INSTANCES["${instance}_status"]}
        local size=${MYSQL_INSTANCES["${instance}_size"]:-"未知"}
        local reliability=${MYSQL_INSTANCES_RELIABILITY["${instance}"]:-0}
        local source=${MYSQL_INSTANCES_SOURCE["${instance}"]:-"unknown"}
        local start_time=${MYSQL_INSTANCES_START_TIME["${instance}"]:-"未知"}
        
        local display_num=$((order + 1))
        
        # 根据可靠性设置颜色
        local reliability_color=$GREEN
        if [[ $reliability -lt 50 ]]; then
            reliability_color=$YELLOW
        fi
        if [[ $reliability -lt 30 ]]; then
            reliability_color=$RED
        fi
        
        echo "$display_num. 实例: $service"
        echo -e "    配置可靠性: ${reliability_color}$reliability/100${NC}"
        echo "    配置来源: $source"
        echo "    状态: $status"
        
        if [[ "$start_time" != "未知" ]]; then
            echo "    启动时间: $start_time"
        fi
        
        echo "    数据目录: $datadir"
        echo "    大小: $size"
        echo "    端口: $port"
        echo "    Socket: $socket"
        [[ -n "$pid" ]] && echo "    PID: $pid"
        
        # 显示配置文件信息
        local conf_files_str=${MYSQL_INSTANCES_CONF_FILES["${instance}"]:-}
        if [[ -n "$conf_files_str" ]]; then
            IFS='|' read -ra conf_files <<< "$conf_files_str"
            echo "    相关配置文件:"
            for conf in "${conf_files[@]}"; do
                local type="${conf%%:*}"
                local path="${conf#*:}"
                if [[ -f "$path" ]]; then
                    local mtime=$(stat -c %y "$path" 2>/dev/null || echo "未知")
                    echo "      - $type: $path"
                    echo "        修改时间: $mtime"
                fi
            done
        fi
        
        # 显示警告信息
        if [[ "$service" =~ ^wild_mysqld_ ]]; then
            echo -e "    ${RED}⚠ 野生实例（无systemd配置）${NC}"
        fi
        
        if [[ "$status" == "activating" ]] || [[ "$status" == "failed" ]]; then
            echo -e "    ${YELLOW}⚠ 服务状态异常${NC}"
        fi

        if [[ "${MYSQL_INSTANCES["${instance}_is_duplicate"]:-}" == "true" ]]; then
            local primary_instance=${MYSQL_INSTANCES["${instance}_duplicate_of"]}
            local primary_service=${MYSQL_INSTANCES["${primary_instance}_service"]}
            echo -e "    ${RED}⚠ 重复实例（主实例: $primary_service）${NC}"
        fi
        
        echo ""
    done
    
    # 显示重复数据目录警告（沿用之前的代码）
    # 如果有重复，显示警告
    local has_duplicates=false
    for datadir in "${!datadir_count[@]}"; do
        if [[ ${datadir_count["$datadir"]} -gt 1 ]]; then
            if [[ "$has_duplicates" == false ]]; then
                echo -e "${YELLOW}⚠ 发现重复数据目录：${NC}"
                has_duplicates=true
            fi
            echo -e "  ${RED}$datadir${NC} 被以下实例共享："
            for instance in ${datadir_services["$datadir"]}; do
                local svc=${MYSQL_INSTANCES["${instance}_service"]}
                local stat=${MYSQL_INSTANCES["${instance}_status"]}
                echo -e "    - $svc (状态: $stat)"
            done
            echo ""
        fi
    done
    
    if [[ "$has_duplicates" == true ]]; then
        echo -e "${RED}警告：多个实例共享同一数据目录可能导致数据损坏！${NC}"
        echo "建议迁移前先确认正确的服务实例。"
        echo ""
    fi
    
    if [[ $LIST_ONLY == "true" ]]; then
        echo_quote
        show_storage_info
        log "INFO" "仅列出实例信息，脚本结束。"
        end_the_batch
    fi

    local choice=0
    local total_instances=${#INSTANCE_NAMES[@]}
    
    while [[ ! $choice =~ ^[0-9]+$ ]] || [[ $choice -lt 1 ]] || [[ $choice -gt $total_instances ]]; do
        read -p "请选择要迁移的实例编号 (1-$total_instances): " choice
    done
    
    # 根据排序后的顺序获取实际实例
    local actual_index=${sorted_indices[$((choice-1))]}
    CURRENT_INSTANCE=${INSTANCE_NAMES[$actual_index]}
    
    echo_quote
    log "INFO" "已选择实例: $CURRENT_INSTANCE"
    
    # 显示选定实例的详细信息
    show_selected_instance_details
}

# 显示选定实例的详细信息
show_selected_instance_details() {
    local instance="$CURRENT_INSTANCE"
    local service=${MYSQL_INSTANCES["${instance}_service"]}
    local reliability=${MYSQL_INSTANCES_RELIABILITY["${instance}"]:-0}
    echo_quote
    local datadir=${MYSQL_INSTANCES["${instance}_datadir"]}
    local port=${MYSQL_INSTANCES["${instance}_port"]}
    local socket=${MYSQL_INSTANCES["${instance}_socket"]}
    local service_file=${MYSQL_INSTANCES["${instance}_service_file"]}
    local pid=${MYSQL_INSTANCES["${instance}_pid"]}
    local status=${MYSQL_INSTANCES["${instance}_status"]}
    local size=${MYSQL_INSTANCES["${instance}_size"]:-"未知"}
    local reliability=${MYSQL_INSTANCES_RELIABILITY["${instance}"]:-0}
    local source=${MYSQL_INSTANCES_SOURCE["${instance}"]:-"unknown"}
    local start_time=${MYSQL_INSTANCES_START_TIME["${instance}"]:-"未知"}
    
    echo_quote
    echo -e "${CYAN}选定实例详细信息: $service${NC}"
    echo_quote
    
    echo "配置可靠性评分: $reliability/100"
    echo ""
    
    # 显示配置来源分析
    if [[ $reliability -lt 50 ]]; then
        echo -e "${YELLOW}⚠ 配置可靠性较低，请谨慎操作${NC}"
        echo "可能的原因："
        if [[ "${MYSQL_INSTANCES_SOURCE["${instance}"]}" == "$CONF_SOURCE_INFERRED" ]]; then
            echo "  - 配置信息为推断得出"
        fi
        if [[ "${MYSQL_INSTANCES["${instance}_service_file"]}" == "无systemd配置" ]]; then
            echo "  - 没有systemd服务配置"
        fi
        if [[ -z "${MYSQL_INSTANCES["${instance}_pid"]}" ]]; then
            echo "  - 实例当前未运行"
        fi
        echo ""
    fi

     # 根据可靠性设置颜色
    local reliability_color=$GREEN
    if [[ $reliability -lt 50 ]]; then
        reliability_color=$YELLOW
    fi
    if [[ $reliability -lt 30 ]]; then
        reliability_color=$RED
    fi
        
    echo "$display_num. 实例: $service"
    echo -e "    配置可靠性: ${reliability_color}$reliability/100${NC}"
    echo "    配置来源: $source"
    echo "    状态: $status"
    
    if [[ "$start_time" != "未知" ]]; then
        echo "    启动时间: $start_time"
    fi
    
    echo "    数据目录: $datadir"
    echo "    大小: $size"
    echo "    端口: $port"
    echo "    Socket: $socket"
    [[ -n "$pid" ]] && echo "    PID: $pid"
    
    # 显示配置文件信息
    local conf_files_str=${MYSQL_INSTANCES_CONF_FILES["${instance}"]:-}
    if [[ -n "$conf_files_str" ]]; then
        IFS='|' read -ra conf_files <<< "$conf_files_str"
        echo "    相关配置文件:"
        for conf in "${conf_files[@]}"; do
            local type="${conf%%:*}"
            local path="${conf#*:}"
            if [[ -f "$path" ]]; then
                local mtime=$(stat -c %y "$path" 2>/dev/null || echo "未知")
                echo "      - $type: $path"
                echo "        修改时间: $mtime"
            fi
        done
    fi
    
    # 询问是否继续
    read -p "确认使用此实例进行迁移? (yes/no): " confirm_instance
    
    if [[ ! "$confirm_instance" =~ ^(yes|YES|y|Y)$ ]]; then
        log "INFO" "用户重新选择实例"
        select_instance_enhanced  # 重新选择
    fi
}

# 收集实例历史信息
collect_instance_history() {
    local instance_name="$1"
    local service=${MYSQL_INSTANCES["${instance_name}_service"]}
    local pid=${MYSQL_INSTANCES["${instance_name}_pid"]}
    
    echo_quote
    echo -e "${CYAN}收集实例历史信息: $service${NC}"
    echo_quote
    
    # 1. systemd日志
    if [[ "$service" != "wild_mysqld_"* ]] && [[ "$service" != "未知" ]]; then
        echo "systemd服务日志（最近5条）:"
        sudo journalctl -u "$service" -n 5 --no-pager 2>/dev/null || echo "无法获取服务日志"
        echo ""
    fi
    
    # 2. 进程运行时间
    if [[ -n "$pid" ]] && [[ "$pid" -gt 0 ]]; then
        local start_time=$(stat -c %Y "/proc/$pid" 2>/dev/null || echo 0)
        if [[ $start_time -gt 0 ]]; then
            local now=$(date +%s)
            local uptime=$((now - start_time))
            local uptime_days=$((uptime / 86400))
            local uptime_hours=$(( (uptime % 86400) / 3600 ))
            echo "进程运行时间: ${uptime_days}天 ${uptime_hours}小时"
        fi
    fi
    
    # 3. 检查MySQL错误日志位置
    local datadir=${MYSQL_INSTANCES["${instance_name}_datadir"]}
    local error_logs=(
        "$datadir/error.log"
        "$datadir/${HOSTNAME}.err"
        "$datadir/mysqld.log"
        "/var/log/mysql/error.log"
        "/var/log/mysqld.log"
        "/var/log/mariadb/mariadb.log"
    )
    
    echo "检查错误日志:"
    for log_file in "${error_logs[@]}"; do
        if [[ -f "$log_file" ]]; then
            echo "  ✓ 发现错误日志: $log_file"
            echo "    大小: $(stat -c %s "$log_file" 2>/dev/null || echo 0) 字节"
            echo "    修改时间: $(stat -c %y "$log_file" 2>/dev/null || echo '未知')"
            
            # 显示最后几条错误（如果有）
            if sudo tail -n 3 "$log_file" 2>/dev/null | grep -q -i "error\|warning\|fail"; then
                echo "    最近错误:"
                sudo tail -n 3 "$log_file" 2>/dev/null | grep -i "error\|warning\|fail" | head -3
            fi
        fi
    done
    
    # 4. 检查备份历史
    echo ""
    echo "检查备份痕迹:"
    local backup_patterns=(
        "*backup*"
        "*dump*"
        "*.sql"
        "*.sql.gz"
        "*.bak"
    )
    
    for pattern in "${backup_patterns[@]}"; do
        local backup_files=$(find "$datadir" -name "$pattern" -type f 2>/dev/null | head -3)
        if [[ -n "$backup_files" ]]; then
            echo "  ⚠ 发现备份文件:"
            echo "$backup_files" | while read -r file; do
                echo "    - $file ($(stat -c %y "$file" 2>/dev/null | cut -d' ' -f1))"
            done
        fi
    done
    
    echo_quote
}

# 检测 MySQL 实例
detect_mysql_instances_old() {
    echo_quote
    log "INFO" "检测 MySQL 实例..."
    
    # 清空实例数组
    INSTANCE_NAMES=()
    
    # 查找所有MySQL相关的systemd服务
    local mysql_services=$(systemctl list-unit-files --type=service 2>/dev/null  | grep -E 'mysql|mariadb' | grep -v '@' | awk '{print $1}' |head -10)
    
    # instance_count已经改为全局变量，以便迁移后根据实例数量推荐mysql客户端配置方案
    instance_count=0
    
    for service in $mysql_services; do
        # 获取服务状态
        #local service_status=$(systemctl is-active "$service" 2>/dev/null || echo "inactive")
        local service_status=$(systemctl is-active "$service" 2>/dev/null)
        
        # 如果服务未安装或不存在，跳过
        if [[ "$service_status" == "inactive" ]] && ! systemctl cat "$service" &>/dev/null; then
            continue
        fi
        
        instance_count=$((instance_count + 1))
        local instance_name="instance_${instance_count}"
        INSTANCE_NAMES+=("$instance_name")
        
        # 获取服务文件路径
        local service_file=$(systemctl show "$service" --property=FragmentPath --value 2>/dev/null || echo "未知")
        
        # 获取进程PID（如果正在运行）
        local pid=""
        if [[ "$service_status" == "active" ]]; then
            pid=$(systemctl show "$service" --property=MainPID --value 2>/dev/null | awk '{print $1}')
        fi
        
        # 默认值
        local datadir="/var/lib/mysql"
        local socket="$datadir/mysql.sock"
        local port="3306"
        
        # 从配置文件获取
        if [[ -f "/etc/my.cnf" ]]; then
            local cfg_datadir=$(grep -E "^\s*datadir\s*=" /etc/my.cnf 2>/dev/null | tail -1 | awk -F= '{print $2}' | xargs)
            local cfg_socket=$(grep -E "^\s*socket\s*=" /etc/my.cnf 2>/dev/null | tail -1 | awk -F= '{print $2}' | xargs)
            local cfg_port=$(grep -E "^\s*port\s*=" /etc/my.cnf 2>/dev/null | tail -1 | awk -F= '{print $2}' | xargs)
            [[ -n "$cfg_datadir" ]] && datadir="$cfg_datadir"
            [[ -n "$cfg_socket" ]] && socket="$cfg_socket"
            [[ -n "$cfg_port" ]] && port="$cfg_port"
        fi
        
        if [[ -f "/etc/my.cnf.d/mysql-server.cnf" ]]; then
            local cfg_datadir=$(grep -E "^\s*datadir\s*=" /etc/my.cnf.d/mysql-server.cnf 2>/dev/null | tail -1 | awk -F= '{print $2}' | xargs)
            local cfg_socket=$(grep -E "^\s*socket\s*=" /etc/my.cnf.d/mysql-server.cnf 2>/dev/null | tail -1 | awk -F= '{print $2}' | xargs)
            local cfg_port=$(grep -E "^\s*port\s*=" /etc/my.cnf.d/mysql-server.cnf 2>/dev/null | tail -1 | awk -F= '{print $2}' | xargs)
            [[ -n "$cfg_datadir" ]] && datadir="$cfg_datadir"
            [[ -n "$cfg_socket" ]] && socket="$cfg_socket"
            [[ -n "$cfg_port" ]] && port="$cfg_port"
        fi
        
        # 如果服务正在运行，检测实际监听的端口
        if [[ -n "$pid" ]] && [[ "$pid" -gt 0 ]]; then
            local detected_ports=$(sudo ss -tlnp 2>/dev/null | grep "pid=$pid" 2>/dev/null | awk '{print $4}' | cut -d: -f2 | sort -nu | tr '\n' ',' | sed 's/,$//' || echo "")
            if [[ -n "$detected_ports" ]]; then
                port="$detected_ports"
            fi
        fi
        
        # 清理路径
        datadir=$(echo "$datadir" | sed 's:/*$::')

        # 获取数据目录大小
        local dir_size=""
        if [[ -d "$datadir" ]]; then
            dir_size=$(get_dir_size "$datadir")
        fi
        
        # 存储实例信息
        MYSQL_INSTANCES["${instance_name}_service"]="$service"
        MYSQL_INSTANCES["${instance_name}_service_file"]="$service_file"
        MYSQL_INSTANCES["${instance_name}_size"]="$dir_size"
        MYSQL_INSTANCES["${instance_name}_datadir"]="$datadir"
        MYSQL_INSTANCES["${instance_name}_port"]="$port"
        MYSQL_INSTANCES["${instance_name}_socket"]="$socket"
        MYSQL_INSTANCES["${instance_name}_pid"]="$pid"
        MYSQL_INSTANCES["${instance_name}_status"]="$service_status"
        
        log "INFO" "发现实例: $service (状态: $service_status, 端口: $port, 数据目录: $datadir,大小: ${dir_size:-未知})"
    done
    
    # 如果没有找到，尝试默认实例
    if [[ $instance_count -eq 0 ]]; then
        instance_count=1
        local instance_name="instance_1"
        INSTANCE_NAMES+=("$instance_name")
        
        MYSQL_INSTANCES["${instance_name}_service"]="mysqld.service"
        MYSQL_INSTANCES["${instance_name}_service_file"]="未知"
        MYSQL_INSTANCES["${instance_name}_datadir"]="/var/lib/mysql"
        MYSQL_INSTANCES["${instance_name}_port"]="3306"
        MYSQL_INSTANCES["${instance_name}_socket"]="/var/lib/mysql/mysql.sock"
        MYSQL_INSTANCES["${instance_name}_pid"]=""
        MYSQL_INSTANCES["${instance_name}_status"]="unknown"
        
        log "WARN" "未发现明确实例，使用默认配置"
    fi
    
    log "INFO" "共发现 $instance_count 个 MySQL 实例"
}

# 显示实例列表并让用户选择
select_instance() {
    echo_quote
    echo -e "${GREEN}检测到的 MySQL 实例：${NC}"
    echo_quote
    
    for i in "${!INSTANCE_NAMES[@]}"; do
        local instance=${INSTANCE_NAMES[$i]}
        local service=${MYSQL_INSTANCES["${instance}_service"]}
        local datadir=${MYSQL_INSTANCES["${instance}_datadir"]}
        local port=${MYSQL_INSTANCES["${instance}_port"]}
        local socket=${MYSQL_INSTANCES["${instance}_socket"]}
        local service_file=${MYSQL_INSTANCES["${instance}_service_file"]}
        local pid=${MYSQL_INSTANCES["${instance}_pid"]}
        local status=${MYSQL_INSTANCES["${instance}_status"]}
        local size=${MYSQL_INSTANCES["${instance}_size"]:-"未知"}
        
        echo "$((i+1)). 实例: $service"
        echo "    状态: $status"
        echo "    数据目录: $datadir"
        echo "    大小: $size"
        echo "    端口: $port"
        echo "    Socket: $socket"
        [[ -n "$pid" ]] && echo "    PID: $pid"
        [[ "$service_file" != "未知" ]] && echo "    配置文件: $service_file"
        [[ -n "${MYSQL_INSTANCES["${instance}_service_file_path"]}" ]] && [[ "${MYSQL_INSTANCES["${instance}_service_file_path"]}" != "未知" ]] && 
    [[ "${MYSQL_INSTANCES["${instance}_service_file_path"]}" != "<none>" ]] && 
    echo "    服务文件: ${MYSQL_INSTANCES["${instance}_service_file_path"]}"
        echo ""
    done
    
    local choice=0
    while [[ ! $choice =~ ^[0-9]+$ ]] || [[ $choice -lt 1 ]] || [[ $choice -gt ${#INSTANCE_NAMES[@]} ]]; do
        read -p "请选择要迁移的实例编号 (1-${#INSTANCE_NAMES[@]}): " choice
    done
    
    CURRENT_INSTANCE=${INSTANCE_NAMES[$((choice-1))]}
    echo_quote
    log "INFO" "已选择实例: $CURRENT_INSTANCE"
}

# 显示操作命令
show_operations() {
    local service=${MYSQL_INSTANCES["${CURRENT_INSTANCE}_service"]}
    local datadir=${MYSQL_INSTANCES["${CURRENT_INSTANCE}_datadir"]}
    local socket=${MYSQL_INSTANCES["${CURRENT_INSTANCE}_socket"]}
    local port=${MYSQL_INSTANCES["${CURRENT_INSTANCE}_port"]}
    local status=${MYSQL_INSTANCES["${CURRENT_INSTANCE}_status"]}
    local size=${MYSQL_INSTANCES["${CURRENT_INSTANCE}_size"]:-"未知"}
    
    echo_quote
    echo -e "${GREEN}请在其他终端窗口中执行以下操作：${NC}"
    # echo -e "${WHITE}================================================${NC}"
    echo ""
    
    # 1. 逻辑备份命令
    local timestamp=$(date +%Y%m%d_%H%M%S)
    local backup_dir="/tmp/mysql_backup_${service//[^a-zA-Z0-9]/_}_${timestamp}"
    
    echo -e "# 创建备份目录，假设备份文件存放在 ${GREEN} $backup_dir ${NC}"
    echo -e "   mkdir -p ${GREEN} $backup_dir ${NC}"

    echo "1. 备份数据库(表结构、数据、存储过程、函数、触发器、视图等数据库对象)："    
    if [[ "$status" == "active" ]] && [[ -S "$socket" ]]; then
        echo ""
    else  
        echo "   # 注意：实例当前未运行，无法进行逻辑备份。"
    fi

    echo "   下面备份命令仅供参考，请根据实际情况调整："
    echo "   mysqldump --all-databases --events --routines --triggers \\"
    echo "     --single-transaction --flush-logs \\"
    echo "     --socket=\"$socket\" > \"$backup_dir/full_backup.sql\""
    echo ""
    
    # 2. 停止服务命令
    echo "2. 停止 MySQL 服务："
    if [[ "$status" == "active" ]]; then
        echo "   sudo systemctl stop $service"
    else
        echo "   # 实例当前未运行，无需停止"
    fi
    echo ""
    
    # 3. 拷贝命令数据库目录命令
    echo "3. 备份数据库实例文件目录(目录大小: $size)："
    echo "   sudo cp -rvp \"$datadir\" \"${datadir}_backup_${timestamp}\""
    echo ""

    echo -e "4. 将上述备份文件拷贝至异地。${RED}确保存在可用异地副本.${NC} "
    echo ""
    
    # echo -e "${WHITE}================================================${NC}"
    echo "在其他终端执行完上述操作后，返回此窗口继续。"
    #echo_quote

    
}

# 确认备份状态
confirm_backup() {
    local response=""
    while [[ ! "$response" =~ ^(yes|YES|y|Y|no|NO|n|N)$ ]]; do
        echo ""
        echo -e "请${RED}自行${NC}完成数据库备份。${NC}"
        read -p "是否已经自行完成数据库备份? (yes/no): " response
    done
    
    if [[ "$response" =~ ^(no|NO|n|N)$ ]]; then
        log "ERROR" "用户未完成备份，终止操作"
        exit 1
    fi
    
    echo_quote
    log "INFO" "用户确认已完成备份"
}

# 确认服务已停止
confirm_service_stopped() {
    local service=${MYSQL_INSTANCES["${CURRENT_INSTANCE}_service"]}
    local status=${MYSQL_INSTANCES["${CURRENT_INSTANCE}_status"]}
    
    # 如果服务正在运行，询问是否停止
    if [[ "$status" == "active" ]]; then
        echo ""
        echo -e "${YELLOW}检测到MySQL实例正在运行。${NC}"
        read -p "是否要停止服务以继续迁移? (yes/no): " stop_choice
        
        if [[ "$stop_choice" =~ ^(yes|YES|y|Y)$ ]]; then
            log "INFO" "停止服务: $service"
            if sudo systemctl stop "$service"; then
                log "INFO" "服务已停止"
                MYSQL_INSTANCES["${CURRENT_INSTANCE}_status"]="inactive"
            else
                log "ERROR" "停止服务失败"
                exit 1
            fi
        else
            log "ERROR" "用户选择不停止服务，无法继续迁移"
            exit 1
        fi
    else
        log "INFO" "服务已停止，可以继续迁移"
    fi
}

# 智能选择最佳默认目录（修复版）
select_best_default_dir() {
    local candidates=(
        "/data/mysql_instance:/data:10:1"      # 路径:分区:最小空间(GB):优先级
        "/opt/mysql_data:/opt:5:2"
        "$HOME/mysql_data:$HOME:2:3"
        "/var/lib/mysql_new:/var:2:4"
    )
    
    # best_dir 已经改为全局变量
    #local best_dir="/var/lib/mysql_new"
    best_dir="/data/DB/mysql_new"
    local best_score=0
    
    log "DEBUG" "开始选择最佳目录..."
    
    for candidate in "${candidates[@]}"; do
        IFS=':' read -r candidate_dir partition min_gb priority <<< "$candidate"
        
        # 检查分区是否存在
        if [[ ! -d "$partition" ]]; then
            log "DEBUG" "跳过 $candidate_dir: 分区 $partition 不存在"
            continue
        fi
        
        # 检查分区信息
        if ! df "$partition" &>/dev/null; then
            log "DEBUG" "跳过 $candidate_dir: 无法获取分区信息"
            continue
        fi
        
        # 获取可用空间（GB）
        local avail_gb=$(df -BG "$partition" 2>/dev/null | awk 'NR==2 {gsub("G","",$4); print int($4)}')
        if [[ -z "$avail_gb" ]] || [[ ! "$avail_gb" =~ ^[0-9]+$ ]]; then
            avail_gb=0
        fi
        
        # 计算得分
        local score=0
        if [[ $avail_gb -ge $min_gb ]]; then
            # 基础分 + 空间额外分 + 优先级分
            score=$((100 + (avail_gb - min_gb) * 2 + (10 - priority) * 5))
            
            # 如果是独立分区（不是根分区）额外加分
            if [[ "$partition" != "/" && "$partition" != "/home" ]]; then
                score=$((score + 20))
            fi
            
            log "DEBUG" "候选目录: $candidate_dir, 分区: $partition, 可用空间: ${avail_gb}GB, 要求: ${min_gb}GB, 得分: $score"
            
            if [[ $score -gt $best_score ]]; then
                best_score=$score
                best_dir="$candidate_dir"
                log "DEBUG" "更新最佳目录为: $best_dir (得分: $best_score)"
            fi
        else
            log "DEBUG" "候选目录: $candidate_dir, 空间不足 (可用: ${avail_gb}GB, 需要: ${min_gb}GB)"
        fi
    done
    
    log "DEBUG" "最终选择目录: $best_dir (得分: $best_score)"
    #echo "$best_dir"
}

# 验证新目录
validate_new_directory() {
    local new_dir="$1"
    local old_dir="$2"
    
    # 检查是否与旧目录相同
    if [[ "$new_dir" == "$old_dir" ]]; then
        echo -e "${RED}错误：新目录不能与当前目录相同！${NC}"
        return 1
    fi
    
    # 检查路径格式
    if [[ ! "$new_dir" =~ ^/ ]]; then
        echo -e "${RED}错误：请输入绝对路径（以/开头）${NC}"
        return 1
    fi
    
    # 检查路径长度
    if [[ ${#new_dir} -lt 3 ]]; then
        echo -e "${RED}错误：路径太短${NC}"
        return 1
    fi
    
    # 检查是否系统关键目录
    local critical_dirs=("/" "/etc" "/bin" "/sbin" "/lib" "/lib64" "/usr" "/boot")
    for dir in "${critical_dirs[@]}"; do
        if [[ "$new_dir" == "$dir" ]] || [[ "$new_dir" =~ ^$dir/ ]]; then
            echo -e "${RED}警告：不建议使用系统关键目录 ($dir)${NC}"
            read -p "确认要继续吗? (yes/no): " confirm
            if [[ ! "$confirm" =~ ^(yes|YES|y|Y)$ ]]; then
                return 1
            fi
            break
        fi
    done
    
    # 检查父目录是否存在
    local parent_dir=$(dirname "$new_dir")
    if [[ ! -d "$parent_dir" ]]; then
        echo "父目录不存在: $parent_dir"
        read -p "是否创建? (yes/no): " create_parent
        if [[ "$create_parent" =~ ^(yes|YES|y|Y)$ ]]; then
            sudo mkdir -p "$parent_dir"
            if [[ $? -ne 0 ]]; then
                echo -e "${RED}创建父目录失败${NC}"
                return 1
            fi
        else
            return 1
        fi
    fi
    
    # 检查磁盘空间
    local parent_partition=$(df "$parent_dir" 2>/dev/null | awk 'NR==2 {print $1}')
    local avail_kb=$(df "$parent_dir" --output=avail 2>/dev/null | tail -1 | tr -d ' ')
    local min_space_kb=$((2 * 1024 * 1024))  # 2GB最小空间
    
    if [[ $avail_kb -lt $min_space_kb ]]; then
        echo -e "${YELLOW}警告：分区 $parent_partition 可用空间不足（当前: ${avail_kb}KB，建议至少: ${min_space_kb}KB）${NC}"
        read -p "确认要继续吗? (yes/no): " confirm
        if [[ ! "$confirm" =~ ^(yes|YES|y|Y)$ ]]; then
            return 1
        fi
    fi
    
    return 0
}


# 检查目录权限
check_directory_permissions() {
    local dir="$1"
    
    echo "检查目录权限: $dir"
    
    # 检查当前权限
    local current_owner=$(stat -c '%U:%G' "$dir" 2>/dev/null || echo "unknown")
    echo "当前所有者: $current_owner"
    
    # 建议设置
    echo "建议设置为: mysql:mysql"
    
    # 如果是根目录或系统目录，显示警告
    if [[ "$dir" =~ ^/(etc|var|usr|lib|bin|sbin) ]]; then
        echo -e "${YELLOW}注意：这是系统目录，修改权限可能影响系统功能${NC}"
    fi
}

# 路径处理工具函数
normalize_path() {
    local path="$1"
    
    # 移除末尾斜杠
    path=$(echo "$path" | sed 's:/*$::')
    
    # 移除重复斜杠（可选）
    path=$(echo "$path" | sed 's://*:/:g')
    
    # 解析相对路径为绝对路径
    if [[ "$path" != /* ]]; then
        path="$(pwd)/$path"
    fi
    
    echo "$path"
}

# 路径比较函数
paths_equal() {
    local path1=$(normalize_path "$1")
    local path2=$(normalize_path "$2")
    
    # 尝试使用 realpath 解析符号链接
    if command -v realpath &>/dev/null; then
        path1=$(realpath -m "$path1" 2>/dev/null || echo "$path1")
        path2=$(realpath -m "$path2" 2>/dev/null || echo "$path2")
    fi
    
    [[ "$path1" == "$path2" ]]
}

# 显示磁盘信息并选择新目录
select_new_directory() {
    echo_quote
    echo -e "${GREEN}选择新的数据目录：${NC}"
    echo "================================================"
    
    # 显示磁盘信息
    echo "当前磁盘使用情况："
    echo "----------------------------------------"
    lsblk 2>/dev/null || echo "lsblk 不可用，使用 df 显示信息"
    echo "----------------------------------------"
    df -h
    echo "----------------------------------------"
    lsblk -af
    echo "----------------------------------------"

    
    local datadir=${MYSQL_INSTANCES["${CURRENT_INSTANCE}_datadir"]}
    echo "当前数据目录: $datadir"
    echo -e "${YELLOW}可用空间: $(df -h "$datadir" | awk 'NR==2 {print $4}')${NC}"
    echo -e "当前实例目录大小：${RED}${MYSQL_INSTANCES["${CURRENT_INSTANCE}_size"]:-未知}${NC}"
    echo ""
    
    # 智能选择默认目录
    select_best_default_dir
    local default_dir=${best_dir}
    
    echo  ""
    echo -e "${GREEN}推荐的目录选择：${NC}"
    echo "1. 数据专用目录: /data/mysql_instance (如果 /data 分区存在)"
    echo "2. 用户目录: $HOME/mysql_data (如果在用户分区)"
    echo "3. 临时方案: /opt/data/DB/mysql_new"
    echo ""
    
    echo -e "${YELLOW}建议新目录：$default_dir${NC}"
    echo "（基于磁盘空间和分区类型自动选择）"
    echo ""
    
    local new_dir=""
    read -p "请输入新的数据目录路径 [$default_dir]: " input_dir
    new_dir=${input_dir:-$default_dir}
    
    # 清理路径（移除末尾的/）
    new_dir="${new_dir%/}"
    echo "规范化后的路径: $new_dir"
    
    # 验证输入
    if ! validate_new_directory "$new_dir" "$datadir"; then
        new_dir=""
    fi
    
    # 确认选择
    echo ""
    echo -e "${CYAN}您选择的新目录:${NC} $new_dir"
    
     # 确认选择后，立即创建目录以获取准确信息
    read -p "确认使用此目录? (yes/no): " confirm
    if [[ "$confirm" =~ ^(yes|YES|y|Y)$ ]]; then
        # 创建目录
        if [[ ! -d "$new_dir" ]]; then
            echo_quote
            echo "创建目录: $new_dir"
            sudo mkdir -p "$new_dir"
            if [[ $? -ne 0 ]]; then
                log "ERROR" "创建目录失败: $new_dir"
                return 1
            fi
        fi
        
        # 现在获取准确的磁盘信息
        echo ""
        echo -e "${CYAN}磁盘空间信息：${NC}"
        if df -h "$new_dir" &>/dev/null; then
            local disk_info=$(df -h "$new_dir" | awk 'NR==2 {
                printf("设备: %s\n", $1);
                printf("大小: %s, 已用: %s, 可用: %s\n", $2, $3, $4);
                printf("使用率: %s, 挂载点: %s", $5, $6);
            }')
            echo "$disk_info"
            
            # 检查是否有足够空间（至少2GB）
            local available_kb=$(df -k "$new_dir" | awk 'NR==2 {print $4}')
            local min_space_kb=$((2 * 1024 * 1024))  # 2GB
            if [[ $available_kb -lt $min_space_kb ]]; then
                echo -e "${YELLOW}警告：可用空间不足（当前: $((available_kb/1024/1024))GB，建议至少: 2GB）${NC}"
                read -p "是否继续? (yes/no): " space_confirm
                if [[ ! "$space_confirm" =~ ^(yes|YES|y|Y)$ ]]; then
                    return 1
                fi
            fi
        else
            echo -e "${YELLOW}警告：无法获取磁盘信息${NC}"
        fi
    else
        return 1
    fi

    # 预检查目录权限
    echo "检查目录权限..."
    check_directory_permissions "$new_dir"
    
    NEW_DATA_DIR="$new_dir"
    log "INFO" "新数据目录设置为: $NEW_DATA_DIR"
    return 0
}

# 在 execute_data_copy 函数开始前添加检查
check_data_directory_conflicts() {
    local datadir=${MYSQL_INSTANCES["${CURRENT_INSTANCE}_datadir"]}
    local service=${MYSQL_INSTANCES["${CURRENT_INSTANCE}_service"]}
    
    # 检查是否有其他服务在使用同一数据目录
    local conflicting_services=()
    
    for name in "${INSTANCE_NAMES[@]}"; do
        if [[ "$name" != "$CURRENT_INSTANCE" ]]; then
            local other_datadir=${MYSQL_INSTANCES["${name}_datadir"]}
            local other_service=${MYSQL_INSTANCES["${name}_service"]}
            local other_status=${MYSQL_INSTANCES["${name}_status"]}
            
            if [[ "$other_datadir" == "$datadir" ]] && [[ "$other_status" == "active" ]]; then
                conflicting_services+=("$other_service")
            fi
        fi
    done
    
    if [[ ${#conflicting_services[@]} -gt 0 ]]; then
        echo -e "${RED}⚠ 发现活跃的冲突实例！${NC}"
        echo "数据目录 $datadir 正被以下服务使用："
        for svc in "${conflicting_services[@]}"; do
            echo "  - $svc"
        done
        echo ""
        echo -e "${YELLOW}建议先停止这些服务再继续迁移：${NC}"
        for svc in "${conflicting_services[@]}"; do
            echo "  sudo systemctl stop $svc"
        done
        echo ""
        
        read -p "是否继续？(yes/no): " continue_migration
        if [[ ! "$continue_migration" =~ ^(yes|YES|y|Y)$ ]]; then
            log "ERROR" "用户取消迁移：存在活跃冲突实例"
            exit 1
        fi
    fi
}

# 简化 execute_data_copy，专注于复制数据
execute_data_copy() {
    local datadir=${MYSQL_INSTANCES["${CURRENT_INSTANCE}_datadir"]}

    echo ""
    echo -e "${GREEN}即将拷贝数据文件：${NC}"
    echo "================================================"
    echo "原数据目录: $datadir"
    echo "新数据目录: $NEW_DATA_DIR"
    echo "================================================"
    # echo ""
    
    # echo "执行以下命令进行拷贝："
    # echo "1. 创建目标目录："
    # echo "   sudo mkdir -p \"$NEW_DATA_DIR\""
    # echo ""
    # echo "2. 拷贝数据（保持权限和属性）："
    # echo "   sudo rsync -av --progress --checksum \\"
    # echo "     --perms --owner --group --times \\"
    # echo "     \"$datadir/\" \"$NEW_DATA_DIR/\""
    # echo ""
    
    #echo "是否立即执行拷贝? (yes/no)"
    read -p "是否立即执行迁移拷贝? (yes/no) > " execute_copy
    echo_quote
    if [[ "$execute_copy" =~ ^(yes|YES|y|Y)$ ]]; then
        log "INFO" "开始拷贝数据文件..."
    else
        log "INFO" "用户取消了操作。"
        return 1
    fi
    
    # echo ""
    # echo -e "${GREEN}拷贝数据文件...${NC}"
    # echo "================================================"
    # echo "原数据目录: $datadir"
    # echo "新数据目录: $NEW_DATA_DIR"
    # echo ""
    
    # 显示原目录属性（供参考）
    echo "原目录属性参考:"
    echo "所有者: $(stat -c '%U:%G' "$datadir" 2>/dev/null || echo '未知')"
    echo "权限: $(stat -c '%a' "$datadir" 2>/dev/null || echo '未知')"
    if command -v ls &>/dev/null && ls -dZ "$datadir" &>/dev/null 2>&1; then
        echo "SELinux: $(ls -dZ "$datadir" 2>/dev/null | awk '{print $1}')"
    fi
    echo ""
    
    # 创建目标目录的父目录（如果需要）
    local parent_dir=$(dirname "$NEW_DATA_DIR")
    if [[ ! -d "$parent_dir" ]]; then
        echo "创建父目录: $parent_dir"
        sudo mkdir -p "$parent_dir"
    fi
    
    echo_quote
    
    # 使用智能的 rsync 命令
    echo "执行 rsync 复制..."
    
    # 构建 rsync 命令，尽可能保留属性
    local rsync_cmd="sudo rsync -av --progress --checksum --perms --owner --group --times"
    
    # 检测并添加扩展属性支持
    if rsync --help 2>&1 | grep -q -- "--xattrs"; then
        rsync_cmd="$rsync_cmd -X"
        echo "✓ 启用扩展属性支持 (-X)"
    fi
    
    # 检测并添加 ACLs 支持
    if rsync --help 2>&1 | grep -q -- "--acls"; then
        rsync_cmd="$rsync_cmd -A"
        echo "✓ 启用 ACLs 支持 (-A)"
    fi
    
    echo "执行命令: $rsync_cmd \"$datadir/\" \"$NEW_DATA_DIR/\""
    echo ""
    
    # 执行拷贝
    if eval "$rsync_cmd \"$datadir/\" \"$NEW_DATA_DIR/\""; then
        log "INFO" "数据拷贝完成"
        echo -e "${GREEN}✓ 数据拷贝完成${NC}"
        echo_quote
        return 0
    else
        log "ERROR" "数据拷贝失败"
        echo -e "${RED}✗ 数据拷贝失败${NC}"
        echo_quote
        return 1
    fi
}

# 对比并修复属性差异
compare_and_fix_attributes() {
    local source_dir="$1"
    local target_dir="$2"

    # 验证目录存在
    if [[ ! -d "$source_dir" ]]; then
        echo "错误：源目录不存在: $source_dir" >&2
        return 1
    fi
    
    if [[ ! -d "$target_dir" ]]; then
        echo "错误：目标目录不存在: $target_dir" >&2
        return 1
    fi
    
    echo ""
    echo -e "${CYAN}对比和修复属性差异...${NC}"
    echo "源目录: $source_dir"
    echo "目标目录: $target_dir"
    echo ""
    
    local total_fixes=0
    local skipped_fixes=0
    
    # 1. 检查并修复基本权限差异
    echo "1. 检查基本权限..."
    local source_owner=$(stat -c '%U:%G' "$source_dir" 2>/dev/null)
    local target_owner=$(stat -c '%U:%G' "$target_dir" 2>/dev/null)
    
    if [[ "$source_owner" != "$target_owner" ]]; then
        echo "  所有者差异: 源[$source_owner] ≠ 目标[$target_owner]"
        echo "  修复所有者..."
        sudo chown -R "${source_owner%:*}" "$target_dir"
        sudo chown "${source_owner%:*}" "$target_dir"  # 目录本身
        echo "  ✓ 已修复所有者"
        total_fixes=$((total_fixes + 1))
    else
        echo "  ✓ 所有者一致: $source_owner"
        skipped_fixes=$((skipped_fixes + 1))
    fi
    
    # 2. 检查并修复SELinux上下文
    echo ""
    echo "2. 检查SELinux上下文..."
    if command -v getenforce &>/dev/null && [[ "$(getenforce)" != "Disabled" ]]; then
        if command -v ls &>/dev/null && ls -dZ "$source_dir" &>/dev/null 2>&1; then
            local source_context=$(ls -dZ "$source_dir" 2>/dev/null | awk '{print $1}')
            local target_context=$(ls -dZ "$target_dir" 2>/dev/null | awk '{print $1}')
            
            if [[ "$source_context" != "$target_context" ]]; then
                echo "  SELinux差异: 源[$source_context] ≠ 目标[$target_context]"
                echo "  修复SELinux上下文..."
                
                # 尝试多种方法
                if command -v chcon &>/dev/null; then
                    sudo chcon -R --reference="$source_dir" "$target_dir"
                    echo "  ✓ 使用 chcon --reference 修复"
                elif [[ "$source_context" =~ :mysqld_db_t: ]]; then
                    sudo chcon -R -t mysqld_db_t "$target_dir"
                    echo "  ✓ 使用 chcon -t mysqld_db_t 修复"
                fi
                total_fixes=$((total_fixes + 1))
            else
                echo "  ✓ SELinux上下文一致: $source_context"
                skipped_fixes=$((skipped_fixes + 1))
            fi
        else
            echo "  ⚠ 无法获取SELinux上下文，尝试设置默认值..."
            sudo chcon -R -t mysqld_db_t "$target_dir" 2>/dev/null || \
            sudo restorecon -R "$target_dir" 2>/dev/null
            total_fixes=$((total_fixes + 1))
        fi
    else
        echo "  ⚠ SELinux已禁用或不可用，跳过"
        skipped_fixes=$((skipped_fixes + 1))
    fi
    
    # 3. 检查并修复ACLs（如果系统支持）
    echo ""
    echo "3. 检查ACLs..."
    if command -v getfacl &>/dev/null; then
        # 检查源目录是否有ACLs
        local source_acls_count=$(getfacl -p "$source_dir" 2>/dev/null | grep -c "^[^#]")
        local target_acls_count=$(getfacl -p "$target_dir" 2>/dev/null | grep -c "^[^#]")
        
        if [[ $source_acls_count -gt 2 ]] && [[ $source_acls_count -ne $target_acls_count ]]; then
            echo "  ACLs差异: 源[$source_acls_count条] ≠ 目标[$target_acls_count条]"
            echo "  修复ACLs..."
            
            if command -v setfacl &>/dev/null; then
                # 备份当前ACLs
                local acl_backup="/tmp/mysql_acl_backup_$$.acl"
                getfacl -p "$source_dir" > "$acl_backup"
                
                # 应用ACLs到目标
                setfacl --restore="$acl_backup" 2>/dev/null
                rm -f "$acl_backup"
                
                echo "  ✓ 使用 setfacl 修复ACLs"
                total_fixes=$((total_fixes + 1))
            else
                echo "  ⚠ setfacl 命令不可用，无法修复ACLs"
                skipped_fixes=$((skipped_fixes + 1))
            fi
        else
            echo "  ✓ ACLs一致或无ACLs"
            skipped_fixes=$((skipped_fixes + 1))
        fi
    else
        echo "  ⚠ getfacl 命令不可用，跳过ACLs检查"
        skipped_fixes=$((skipped_fixes + 1))
    fi
    
    # 4. 检查并修复扩展属性（如果系统支持）
    echo ""
    echo "4. 检查扩展属性..."
    if command -v getfattr &>/dev/null; then
        # 检查源目录是否有扩展属性
        local source_xattrs=$(getfattr -d -m - "$source_dir" 2>/dev/null | grep -v "^#")
        local target_xattrs=$(getfattr -d -m - "$target_dir" 2>/dev/null | grep -v "^#")
        
        if [[ -n "$source_xattrs" ]] && [[ "$source_xattrs" != "$target_xattrs" ]]; then
            echo "  扩展属性存在差异"
            echo "  修复扩展属性..."
            
            if command -v setfattr &>/dev/null; then
                # 复制扩展属性
                getfattr -d -m - "$source_dir" 2>/dev/null | \
                while read line; do
                    if [[ "$line" =~ ^(.*)\=\"(.*)\"$ ]]; then
                        local attr="${BASH_REMATCH[1]}"
                        local value="${BASH_REMATCH[2]}"
                        sudo setfattr -n "$attr" -v "$value" "$target_dir" 2>/dev/null
                    fi
                done
                
                echo "  ✓ 已复制扩展属性"
                total_fixes=$((total_fixes + 1))
            else
                echo "  ⚠ setfattr 命令不可用，无法修复扩展属性"
                skipped_fixes=$((skipped_fixes + 1))
            fi
        else
            echo "  ✓ 扩展属性一致或无扩展属性"
            skipped_fixes=$((skipped_fixes + 1))
        fi
    else
        echo "  ⚠ getfattr 命令不可用，跳过扩展属性检查"
        skipped_fixes=$((skipped_fixes + 1))
    fi
    
    # 5. 检查并修复特殊权限位（setuid, setgid, sticky）
    echo ""
    echo "5. 检查特殊权限位..."
    local source_mode=$(stat -c '%a' "$source_dir" 2>/dev/null)
    local target_mode=$(stat -c '%a' "$target_dir" 2>/dev/null)
    
    # 比较特殊权限位（最后一位数字）
    local source_special=$((source_mode / 1000))
    local target_special=$((target_mode / 1000))
    
    if [[ $source_special -ne $target_special ]]; then
        echo "  特殊权限位差异: 源[$source_mode] ≠ 目标[$target_mode]"
        echo "  修复权限..."
        sudo chmod $source_mode "$target_dir"
        echo "  ✓ 已修复权限位"
        total_fixes=$((total_fixes + 1))
    else
        echo "  ✓ 特殊权限位一致"
        skipped_fixes=$((skipped_fixes + 1))
    fi
    
    # 总结报告
    echo ""
    echo -e "${GREEN}属性对比修复完成${NC}"
    echo_quote
    # echo "========================================"
    echo "总计修复项目: $total_fixes"
    echo "跳过项目: $skipped_fixes"
    
    if [[ $total_fixes -gt 0 ]]; then
        echo -e "${YELLOW}注：已对 $total_fixes 项属性进行了修复${NC}"
    else
        echo -e "${GREEN}✓ 所有属性均已正确复制，无需修复${NC}"
    fi
    
    return 0
}

# 快速检查差异（不实际修复）,该函数并未使用
quick_check_attributes() {
    local source_dir="$1"
    local target_dir="$2"
    
    echo "快速属性差异检查:"
    echo "=================="
    
    # 检查所有者
    local source_owner=$(stat -c '%U:%G' "$source_dir" 2>/dev/null)
    local target_owner=$(stat -c '%U:%G' "$target_dir" 2>/dev/null)
    [[ "$source_owner" != "$target_owner" ]] && echo "⚠ 所有者差异"
    
    # 检查SELinux
    if command -v ls &>/dev/null && ls -dZ "$source_dir" &>/dev/null 2>&1; then
        local source_ctx=$(ls -dZ "$source_dir" 2>/dev/null | awk '{print $1}')
        local target_ctx=$(ls -dZ "$target_dir" 2>/dev/null | awk '{print $1}')
        [[ "$source_ctx" != "$target_ctx" ]] && echo "⚠ SELinux差异"
    fi
    
    # 检查权限
    local source_mode=$(stat -c '%a' "$source_dir" 2>/dev/null)
    local target_mode=$(stat -c '%a' "$target_dir" 2>/dev/null)
    [[ "$source_mode" != "$target_mode" ]] && echo "⚠ 权限差异"
}

# 简化 fix_permissions，调用对比修复函数
fix_permissions() {
    local target_dir="$1"
    local source_dir=${MYSQL_INSTANCES["${CURRENT_INSTANCE}_datadir"]}
    
    echo_quote
    # echo ""
    echo -e "${GREEN}执行权限验证和修复...${NC}"
    
    # 调用对比修复函数
    compare_and_fix_attributes "$source_dir" "$target_dir"
    
    # 额外验证：确保目录可访问
    echo ""
    echo "最终权限验证:"
    echo "目录路径: $target_dir"
    echo "所有者: $(stat -c '%U:%G' "$target_dir" 2>/dev/null || echo '未知')"
    echo "权限: $(stat -c '%a' "$target_dir" 2>/dev/null || echo '未知')"
    
    # 检查SELinux上下文（如果可用）
    if command -v ls &>/dev/null && ls -dZ "$target_dir" &>/dev/null 2>&1; then
        echo "SELinux: $(ls -dZ "$target_dir" 2>/dev/null | awk '{print $1}')"
    fi
    
    return 0
}

# 更新systemd服务配置文件（增强版）
update_systemd_service_config() {
    local service=${MYSQL_INSTANCES["${CURRENT_INSTANCE}_service"]}
    local old_datadir=${MYSQL_INSTANCES["${CURRENT_INSTANCE}_datadir"]}
    local old_socket=${MYSQL_INSTANCES["${CURRENT_INSTANCE}_socket"]}
    local old_pidfile=${MYSQL_INSTANCES["${CURRENT_INSTANCE}_pidfile"]:-""}
    local service_file_path=${MYSQL_INSTANCES["${CURRENT_INSTANCE}_service_file_path"]:-""}
    local new_datadir="$NEW_DATA_DIR"
    local new_socket="$old_socket"
    local new_pidfile="$old_pidfile"
    
    # 计算新的路径
    if [[ "$old_socket" == "$old_datadir/"* ]]; then
        new_socket="${new_datadir}${old_socket#$old_datadir}"
    fi
    
    if [[ -n "$old_pidfile" ]] && [[ "$old_pidfile" == "$old_datadir/"* ]]; then
        new_pidfile="${new_datadir}${old_pidfile#$old_datadir}"
    fi
    
    echo ""
    echo -e "${GREEN}更新systemd服务配置：${NC}"
    echo_quote
    
    # 使用之前存储的服务文件路径，如果不存在则重新查找
    if [[ -z "$service_file_path" ]] || [[ ! -f "$service_file_path" ]]; then
        # 获取服务配置文件路径
        service_file_path=$(systemctl cat "$service" 2>/dev/null | grep -E "^#" | grep "Loaded:" | awk -F'[\(;]' '{print $2}' | sed 's/^\s*//;s/\s*$//' | head -1)
        
        if [[ -z "$service_file_path" ]] || [[ ! -f "$service_file_path" ]]; then
            service_file_path=$(systemctl show "$service" --property=FragmentPath --value 2>/dev/null)
        fi
        
        if [[ -z "$service_file_path" ]] || [[ ! -f "$service_file_path" ]]; then
            log "WARN" "无法获取$service的配置文件，尝试查找..."
            
            local possible_paths=(
                "/etc/systemd/system/$service"
                "/usr/lib/systemd/system/$service"
                "/lib/systemd/system/$service"
            )
            
            for path in "${possible_paths[@]}"; do
                if [[ -f "$path" ]]; then
                    service_file_path="$path"
                    break
                fi
            done
        fi
    fi
    
    if [[ -z "$service_file_path" ]] || [[ ! -f "$service_file_path" ]]; then
        log "ERROR" "未找到$service的配置文件"
        echo -e "${RED}错误：无法找到服务配置文件${NC}"
        echo "请手动检查服务配置："
        echo "  systemctl cat $service"
        return 1
    fi
    
    echo "服务配置文件: $service_file_path"
    echo "配置文件大小: $(stat -c %s "$service_file_path") 字节"
    
    # 1. 展示修改前的文件内容
    echo ""
    echo -e "${CYAN}修改前的配置文件内容：${NC}"
    echo "================================================"
    if [[ -f "$service_file_path" ]] && [[ -r "$service_file_path" ]]; then
        cat -n "$service_file_path" 2>/dev/null || echo "无法读取文件内容"
    else
        echo "文件不可读或不存在"
    fi
    echo "================================================"
    
    # 备份原文件
    local timestamp=$(date +%Y%m%d_%H%M%S)
    local backup_dir="/tmp/mysql_migrate_backup"
    sudo mkdir -p "$backup_dir"
    local backup_file="${backup_dir}/$(basename "$service_file_path").backup.${timestamp}"
    local backup_file_simple="${service_file_path}.backup.${timestamp}"
    
    # 创建两个备份：一个在临时目录，一个在原文件同级目录
    sudo cp "$service_file_path" "$backup_file"
    sudo cp "$service_file_path" "$backup_file_simple"
    
    log "INFO" "已创建配置文件备份:"
    log "INFO" "  1. 临时备份: $backup_file"
    log "INFO" "  2. 同级备份: $backup_file_simple"
    
    echo ""
    echo -e "${YELLOW}重要：配置文件已备份！${NC}"
    echo "备份位置1（临时目录）: $backup_file"
    echo "备份位置2（同级目录）: $backup_file_simple"
    echo ""
    echo -e "${RED}如果迁移失败，可以手动恢复：${NC}"
    echo "  sudo cp \"$backup_file\" \"$service_file_path\""
    echo "  sudo systemctl daemon-reload"
    echo "  sudo systemctl restart $service"
    echo ""
    
    # 获取当前ExecStart行
    local exec_start_line_num=$(grep -n "^ExecStart=" "$service_file_path" | head -1 | cut -d: -f1)
    
    if [[ -z "$exec_start_line_num" ]]; then
        # 如果没有ExecStart=开头的行，查找包含ExecStart的行
        exec_start_line_num=$(grep -n "ExecStart" "$service_file_path" | head -1 | cut -d: -f1)
    fi
    
    if [[ -z "$exec_start_line_num" ]]; then
        log "ERROR" "未找到ExecStart配置"
        echo -e "${YELLOW}警告：未找到ExecStart配置，可能需要手动修改${NC}"
        echo "请检查文件内容并手动添加必要的参数"
        return 1
    fi
    
    # 读取ExecStart行内容
    local exec_start_line=$(sed -n "${exec_start_line_num}p" "$service_file_path")
    echo "当前ExecStart配置: $exec_start_line"
    
    # 记录修改日志
    local modification_log="${backup_dir}/modifications.log"
    echo "=== 修改日志 $(date) ===" >> "$modification_log"
    echo "服务: $service" >> "$modification_log"
    echo "原数据目录: $old_datadir" >> "$modification_log"
    echo "新数据目录: $new_datadir" >> "$modification_log"
    
    # 1. 检查是否需要添加--datadir参数
    echo ""
    echo "1. 检查--datadir参数..."
    
    if [[ ! "$exec_start_line" =~ --datadir ]]; then
        echo "  当前配置中没有--datadir参数，需要添加"
        echo "  将在ExecStart中添加: --datadir=$new_datadir"
        
        # 在mysqld命令后添加--datadir参数
        # 先检查是否有--defaults-file参数，在其后添加
        if [[ "$exec_start_line" =~ --defaults-file ]]; then
            # 在--defaults-file参数后添加--datadir
            local modified_line=$(echo "$exec_start_line" | sed "s|\(--defaults-file=[^ ]*\)|\1 --datadir=$new_datadir|")
            sudo sed -i "${exec_start_line_num}s|.*|$modified_line|" "$service_file_path"
            echo "  ✓ 在--defaults-file后添加了 --datadir=$new_datadir"
            echo "  添加 --datadir=$new_datadir" >> "$modification_log"
        else
            # 在mysqld命令后添加
            local modified_line=$(echo "$exec_start_line" | sed "s|\(/usr/sbin/mysqld\)|\1 --datadir=$new_datadir|")
            sudo sed -i "${exec_start_line_num}s|.*|$modified_line|" "$service_file_path"
            echo "  ✓ 在mysqld命令后添加了 --datadir=$new_datadir"
            echo "  添加 --datadir=$new_datadir" >> "$modification_log"
        fi
    else
        # 更新现有的--datadir参数
        sudo sed -i "${exec_start_line_num}s|--datadir[= ][^ ]*|--datadir=$new_datadir|g" "$service_file_path"
        sudo sed -i "${exec_start_line_num}s|--datadir = [^ ]*|--datadir = $new_datadir|g" "$service_file_path"
        echo "  ✓ 更新--datadir参数: $new_datadir"
        echo "  更新 --datadir 为 $new_datadir" >> "$modification_log"
    fi
    
    # 重新读取更新后的行
    exec_start_line=$(sed -n "${exec_start_line_num}p" "$service_file_path")
    
    # 2. 更新--socket参数
    echo ""
    echo "2. 检查--socket参数..."
    
    if [[ "$exec_start_line" =~ --socket ]]; then
        sudo sed -i "${exec_start_line_num}s|--socket[= ][^ ]*|--socket=$new_socket|g" "$service_file_path"
        sudo sed -i "${exec_start_line_num}s|--socket = [^ ]*|--socket = $new_socket|g" "$service_file_path"
        echo "  ✓ 更新--socket参数: $new_socket"
        echo "  更新 --socket 为 $new_socket" >> "$modification_log"
    else
        # 如果原来没有--socket参数，需要添加
        echo "  当前配置中没有--socket参数"
        read -p "  是否添加--socket参数? (yes/no): " add_socket
        
        if [[ "$add_socket" =~ ^(yes|YES|y|Y)$ ]]; then
            local modified_line=$(echo "$exec_start_line" | sed "s|$| --socket=$new_socket|")
            sudo sed -i "${exec_start_line_num}s|.*|$modified_line|" "$service_file_path"
            echo "  ✓ 添加了 --socket=$new_socket"
            echo "  添加 --socket=$new_socket" >> "$modification_log"
        else
            echo "  ⚠ 跳过添加--socket参数"
            echo "  未添加 --socket 参数" >> "$modification_log"
        fi
    fi
    
    # 3. 更新--pid-file参数
    echo ""
    echo "3. 检查--pid-file参数..."
    
    if [[ "$exec_start_line" =~ --pid-file ]]; then
        if [[ -n "$new_pidfile" ]] && [[ "$new_pidfile" != "$old_pidfile" ]]; then
            sudo sed -i "${exec_start_line_num}s|--pid-file[= ][^ ]*|--pid-file=$new_pidfile|g" "$service_file_path"
            sudo sed -i "${exec_start_line_num}s|--pid-file = [^ ]*|--pid-file = $new_pidfile|g" "$service_file_path"
            echo "  ✓ 更新--pid-file参数: $new_pidfile"
            echo "  更新 --pid-file 为 $new_pidfile" >> "$modification_log"
        fi
    fi
    
    # 4. 更新PIDFile指令
    echo ""
    echo "4. 检查PIDFile指令..."
    
    local pidfile_line_num=$(grep -n "^PIDFile=" "$service_file_path" | head -1 | cut -d: -f1)
    
    if [[ -n "$pidfile_line_num" ]]; then
        local pidfile_line=$(sed -n "${pidfile_line_num}p" "$service_file_path")
        local current_pidfile=$(echo "$pidfile_line" | sed -n 's/^PIDFile=//p')
        
        if [[ "$current_pidfile" == "$old_datadir/"* ]] && [[ -n "$new_pidfile" ]]; then
            sudo sed -i "${pidfile_line_num}s|^PIDFile=.*|PIDFile=$new_pidfile|" "$service_file_path"
            echo "  ✓ 更新PIDFile指令: $new_pidfile"
            echo "  更新 PIDFile 为 $new_pidfile" >> "$modification_log"
        fi
    fi
    
    # 5. 更新ExecStartPre中的路径
    echo ""
    echo "5. 更新ExecStartPre中的路径..."
    
    # 获取所有ExecStartPre行
    local exec_startpre_lines=$(grep -n "^ExecStartPre=" "$service_file_path" || true)
    
    if [[ -n "$exec_startpre_lines" ]]; then
        while IFS= read -r line_info; do
            local line_num=$(echo "$line_info" | cut -d: -f1)
            local line_content=$(echo "$line_info" | cut -d: -f2-)
            
            # 更新路径引用
            if [[ "$line_content" =~ $old_datadir ]]; then
                sudo sed -i "${line_num}s|$old_datadir|$new_datadir|g" "$service_file_path"
                echo "  ✓ 更新ExecStartPre路径引用 (第${line_num}行)"
                echo "  更新 ExecStartPre 第${line_num}行: $old_datadir -> $new_datadir" >> "$modification_log"
                
                # 重新读取并显示更新后的内容
                local updated_line=$(sed -n "${line_num}p" "$service_file_path")
                echo "    更新后: $updated_line"
            fi
        done <<< "$exec_startpre_lines"
    else
        echo "  未找到ExecStartPre配置"
    fi
    
    # 6. 更新其他可能包含路径的配置项
    echo ""
    echo "6. 检查其他配置项..."
    
    # 检查WorkingDirectory
    local workdir_line_num=$(grep -n "^WorkingDirectory=" "$service_file_path" | head -1 | cut -d: -f1)
    
    if [[ -n "$workdir_line_num" ]]; then
        local workdir_line=$(sed -n "${workdir_line_num}p" "$service_file_path")
        
        if [[ "$workdir_line" =~ $old_datadir ]]; then
            local new_workdir="${new_datadir}${workdir_line#*$old_datadir}"
            new_workdir=$(echo "$new_workdir" | sed 's/^WorkingDirectory=//')
            sudo sed -i "${workdir_line_num}s|^WorkingDirectory=.*|WorkingDirectory=$new_workdir|" "$service_file_path"
            echo "  ✓ 更新WorkingDirectory: $new_workdir"
            echo "  更新 WorkingDirectory 为 $new_workdir" >> "$modification_log"
        fi
    fi
    
    # 7. 处理socket清理命令中的路径
    echo ""
    echo "7. 处理socket清理命令..."
    
    # 查找包含socket清理的行
    local socket_clean_lines=$(grep -n "rm.*sock" "$service_file_path" || true)
    
    if [[ -n "$socket_clean_lines" ]]; then
        while IFS= read -r line_info; do
            local line_num=$(echo "$line_info" | cut -d: -f1)
            local line_content=$(echo "$line_info" | cut -d: -f2-)
            
            # 更新路径
            if [[ "$line_content" =~ $old_datadir ]]; then
                # 获取当前行中的socket路径
                local sock_paths=$(echo "$line_content" | grep -o '\"[^\"]*sock[^\"]*\"' | head -5)
                
                for sock_path in $sock_paths; do
                    # 移除引号
                    sock_path=$(echo "$sock_path" | sed 's/^"//;s/"$//')
                    
                    if [[ "$sock_path" == "$old_datadir/"* ]]; then
                        local new_sock_path="${new_datadir}${sock_path#$old_datadir}"
                        sudo sed -i "${line_num}s|\"$sock_path\"|\"$new_sock_path\"|g" "$service_file_path"
                        echo "    ✓ 更新socket路径: $new_sock_path"
                        echo "    更新 socket 路径: $sock_path -> $new_sock_path" >> "$modification_log"
                    fi
                done
            fi
        done <<< "$socket_clean_lines"
    fi
    
    # 8. 重新加载systemd配置
    echo ""
    echo "8. 重新加载systemd配置..."
    
    if sudo systemctl daemon-reload; then
        echo "  ✓ systemd配置已重新加载"
        echo "  执行 systemctl daemon-reload 成功" >> "$modification_log"
    else
        echo "  ⚠ 重新加载systemd配置失败"
        echo "  ⚠ systemctl daemon-reload 失败" >> "$modification_log"
    fi
    
    # 9. 展示修改后的文件内容
    echo ""
    echo -e "${CYAN}修改后的配置文件内容：${NC}"
    echo "================================================"
    if [[ -f "$service_file_path" ]] && [[ -r "$service_file_path" ]]; then
        cat -n "$service_file_path" 2>/dev/null || echo "无法读取文件内容"
    else
        echo "文件不可读或不存在"
    fi
    echo "================================================"
    
    # 10. 显示更新前后的差异
    echo ""
    echo "10. 配置文件变更对比:"
    echo "================================================"
    
    # 获取更新后的ExecStart行
    local updated_exec_start=$(sed -n "${exec_start_line_num}p" "$service_file_path" 2>/dev/null)
    
    echo "原ExecStart:"
    echo "  $exec_start_line"
    echo ""
    echo "新ExecStart:"
    echo "  $updated_exec_start"
    echo ""
    
    if command -v diff &>/dev/null; then
        echo "完整差异对比:"
        sudo diff -u "$backup_file" "$service_file_path" 2>/dev/null | head -200 || echo "无法显示完整差异"
    else
        echo "diff命令不可用，无法显示差异"
    fi
    
    echo "================================================"
    
    # 11. 验证服务配置
    echo ""
    echo "11. 验证服务配置..."
    
    echo "当前服务状态:"
    systemctl status "$service" --no-pager 2>/dev/null | head -20 || echo "无法获取服务状态"
    
    echo ""
    echo "关键配置检查:"
    echo "ExecStart:"
    systemctl show "$service" --property=ExecStart --value 2>/dev/null || echo "无法获取ExecStart"
    
    echo ""
    echo "PIDFile:"
    systemctl show "$service" --property=PIDFile --value 2>/dev/null || echo "无法获取PIDFile"
    
    echo ""
    echo -e "${GREEN}✓ systemd服务配置更新完成${NC}"
    
    # 12. 显示备份和恢复信息
    echo ""
    echo -e "${YELLOW}备份和恢复信息：${NC}"
    echo "================================================"
    echo "配置文件备份位置:"
    echo "  1. $backup_file"
    echo "  2. $backup_file_simple"
    echo ""
    echo "修改日志: $modification_log"
    echo ""
    echo -e "${RED}如果服务无法启动，可以手动恢复：${NC}"
    echo "  1. 恢复配置文件:"
    echo "     sudo cp \"$backup_file\" \"$service_file_path\""
    echo "     sudo systemctl daemon-reload"
    echo "  2. 或者编辑配置文件手动修复"
    echo "     sudo vi \"$service_file_path\""
    echo "================================================"
    
    # 13. 建议重启服务
    echo ""
    echo -e "${YELLOW}建议操作:${NC}"
    echo "配置已更新，建议重启服务以应用更改:"
    echo "  sudo systemctl restart $service"
    echo ""
    echo "或者，脚本将在后续步骤中自动重启服务进行验证。"
    
    return 0
}

# 更新配置文件
update_configurations() {
    local service=${MYSQL_INSTANCES["${CURRENT_INSTANCE}_service"]}
    local datadir=${MYSQL_INSTANCES["${CURRENT_INSTANCE}_datadir"]}
    local socket=${MYSQL_INSTANCES["${CURRENT_INSTANCE}_socket"]}
    local service_file=${MYSQL_INSTANCES["${CURRENT_INSTANCE}_service_file"]}
    
    echo ""
    echo -e "${GREEN}更新配置文件：${NC}"
    # echo "================================================"
    echo_quote
    
    # 计算新的socket路径
    local new_socket="$socket"
    if [[ "$socket" == "$datadir/"* ]]; then
        new_socket="${NEW_DATA_DIR}${socket#$datadir}"
    fi
    
    echo "将更新以下配置："
    echo "1. 数据目录: $datadir -> $NEW_DATA_DIR"
    echo "2. Socket文件: $socket -> $new_socket"
    echo "3. systemd服务配置文件: $service_file"
    
    # 更新MySQL配置文件
    local updated_files=()
    local config_files=("/etc/my.cnf" "/etc/my.cnf.d/mysql-server.cnf")
    
    for config in "${config_files[@]}"; do
        if [[ -f "$config" ]]; then
            # 备份原文件
            local backup_file="${config}.backup.$(date +%Y%m%d_%H%M%S)"
            sudo cp "$config" "$backup_file"
            log "INFO" "已备份配置文件: $config -> $backup_file"
            
            # 更新datadir
            sudo sed -i "s|datadir=$datadir|datadir=$NEW_DATA_DIR|g" "$config"
            sudo sed -i "s|datadir = $datadir|datadir = $NEW_DATA_DIR|g" "$config"
            
            # 更新socket
            sudo sed -i "s|socket=$socket|socket=$new_socket|g" "$config"
            sudo sed -i "s|socket = $socket|socket = $new_socket|g" "$config"

            # echo "更新前后差异："
            # echo "==============================================="
            # diff  "$backup_file" "$config" 
            # echo "==============================================="
            
            updated_files+=("$config")
            log "INFO" "已更新配置文件: $config"
        fi
    done

    # 更新systemd服务配置
    if update_systemd_service_config; then
        updated_files+=("systemd service: $service")
    else
        echo -e "${YELLOW}⚠ systemd服务配置更新可能失败，请手动检查${NC}"
    fi
    
    # 如果socket文件在数据目录外，创建符号链接
    if [[ "$socket" != "$datadir/"* ]] && [[ -e "$socket" ]]; then
        echo ""
        echo "注意：检测到Socket文件在数据目录外。"
        echo "建议创建符号链接："
        echo "sudo ln -sf \"$new_socket\" \"$socket\""
        
        read -p "是否创建符号链接? (yes/no): " create_link
        if [[ "$create_link" =~ ^(yes|YES|y|Y)$ ]]; then
            sudo ln -sf "$new_socket" "$socket"
            log "INFO" "已创建符号链接: $socket -> $new_socket"
        fi
    fi
    
    # echo ""
    echo_quote
    echo -e "${GREEN}配置文件更新完成！${NC}"
    echo "更新的文件："
    for file in "${updated_files[@]}"; do
        echo "  - $file"
    done
    echo_quote

    # 询问是否更新客户端配置
    echo ""
    echo -e "${GREEN}客户端连接配置：${NC}"
    echo_quote
    # echo "================================================"
    echo "当前配置下，连接此实例需要指定参数："
    echo "  mysql -S \"$new_socket\""
    echo ""
    echo "是否更新客户端配置，使 'mysql' 命令可以直接连接？"
    echo "这将在配置文件中添加 [client] 和 [mysql] 段。"
    echo ""
    
    read -p "更新客户端配置? (yes/no): " update_client
    echo_quote
    if [[ "$update_client" =~ ^(yes|YES|y|Y)$ ]]; then
        update_client_configuration "$new_socket"
    else
        show_client_config_instructions "$new_socket"
    fi
}

# 更新客户端配置
update_client_configuration() {
    local new_socket="$1"

    if [ $instance_count -gt 1 ]; then
        echo "之前检测到多个实例，建议为每个实例创建独立的客户端配置段。" 
        configure_multi_instance_support "$new_socket"
    fi
    
    echo ""
    echo "选择要更新的配置文件："
    echo "1. 系统级配置 (/etc/my.cnf.d/mysql-client.cnf) - 影响所有用户"
    echo "2. 用户级配置 (~/.my.cnf) - 仅影响当前用户"
    echo "3. 添加到现有配置文件"
    echo ""
    
    local choice=0
    while [[ ! $choice =~ ^[1-3]$ ]]; do
        read -p "请选择 (1-3): " choice
    done
    echo_quote

    case $choice in
        1)
            # 系统级配置
            local client_config="/etc/my.cnf.d/mysql-client.cnf"
            sudo mkdir -p "$(dirname "$client_config")"
            
            if [[ -f "$client_config" ]]; then
                echo "备份原配置文件: ${client_config}.backup"
                sudo cp "$client_config" "${client_config}.backup.$(date +%Y%m%d_%H%M%S)"
            fi
            
            echo "创建系统级客户端配置..."
            sudo tee "$client_config" > /dev/null << EOF
# MySQL 客户端配置
# 生成时间: $(date)
# 实例: ${MYSQL_INSTANCES["${CURRENT_INSTANCE}_service"]}

[client]
socket = $new_socket

[mysql]
socket = $new_socket
EOF
            echo -e "${GREEN}✓ 已更新系统级客户端配置${NC}"
            ;;
            
        2)
            # 用户级配置
            local client_config="$HOME/.my.cnf"
            
            if [[ -f "$client_config" ]]; then
                echo "备份原配置文件: ${client_config}.backup"
                cp "$client_config" "${client_config}.backup.$(date +%Y%m%d_%H%M%S)"
            fi
            
            echo "创建用户级客户端配置..."
            tee "$client_config" > /dev/null << EOF
# MySQL 客户端配置
# 生成时间: $(date)
# 实例: ${MYSQL_INSTANCES["${CURRENT_INSTANCES}_service"]}

[client]
socket = $new_socket

[mysql]
socket = $new_socket
EOF
            echo -e "${GREEN}✓ 已更新用户级客户端配置${NC}"
            ;;
            
        3)
            # 添加到现有配置文件
            local target_file=""
            read -p "输入要更新的配置文件路径: " target_file
            
            if [[ -f "$target_file" ]]; then
                # 检查是否已存在 [client] 段
                if grep -q "^\[client\]" "$target_file"; then
                    # 更新现有 [client] 段
                    sudo sed -i "/^\[client\]/,/^\[/ s|^socket\s*=.*|socket = $new_socket|" "$target_file"
                    
                    # 如果 [client] 段中没有socket设置，添加一个
                    if ! grep -A5 "^\[client\]" "$target_file" | grep -q "^socket"; then
                        sudo sed -i "/^\[client\]/a socket = $new_socket" "$target_file"
                    fi
                else
                    # 添加新的 [client] 段
                    echo -e "\n[client]\nsocket = $new_socket\n" | sudo tee -a "$target_file" > /dev/null
                fi
                
                # 检查是否已存在 [mysql] 段
                if grep -q "^\[mysql\]" "$target_file"; then
                    sudo sed -i "/^\[mysql\]/,/^\[/ s|^socket\s*=.*|socket = $new_socket|" "$target_file"
                else
                    echo -e "\n[mysql]\nsocket = $new_socket\n" | sudo tee -a "$target_file" > /dev/null
                fi
                
                echo -e "${GREEN}✓ 已更新配置文件: $target_file${NC}"
            else
                echo -e "${RED}文件不存在: $target_file${NC}"
            fi
            ;;
    esac
    
    echo ""
    echo "测试客户端配置："
    echo "尝试运行: mysql -e \"SELECT '连接成功' as status;\""
    echo "如果配置正确，应该可以连接成功。"
    echo_quote
    echo_quote
}

# 多实例Client环境配置
configure_multi_instance_support() {
    local new_socket="$1"
    local instance_name=""
    
    echo ""
    echo -e "${GREEN}多实例环境配置：${NC}"
    echo "================================================"
    echo "当前系统中有多个MySQL实例吗？"
    echo ""
    read -p "是否为此实例创建专用配置段？ (yes/no): " multi_instance
    
    if [[ "$multi_instance" =~ ^(yes|YES|y|Y)$ ]]; then
        # 获取实例标识名
        read -p "输入此实例的标识名（如：migrated、app1等）: " instance_name
        
        if [[ -n "$instance_name" ]]; then
            # 创建多实例配置
            local multi_config="/etc/my.cnf.d/mysql-${instance_name}.cnf"
            
            sudo tee "$multi_config" > /dev/null << EOF
# MySQL 实例配置: $instance_name
# 生成时间: $(date)
# 服务: ${MYSQL_INSTANCES["${CURRENT_INSTANCE}_service"]}

[client:$instance_name]
socket = $new_socket
user = root
#port = 3306  # 如果使用TCP

[mysql:$instance_name]
socket = $new_socket
prompt = "mysql [$instance_name]> "
EOF
            
            echo -e "${GREEN}✓ 已创建多实例配置文件: $multi_config${NC}"
            echo ""
            echo "使用方式："
            echo "  mysql --defaults-group-suffix=:$instance_name"
            echo ""
            echo "建议创建别名："
            echo "  alias mysql-$instance_name=\"mysql --defaults-group-suffix=:$instance_name\""
            
            # 询问是否创建别名
            read -p "是否在 ~/.bashrc 中创建别名? (yes/no): " create_alias
            if [[ "$create_alias" =~ ^(yes|YES|y|Y)$ ]]; then
                echo "alias mysql-$instance_name=\"mysql --defaults-group-suffix=:$instance_name\"" >> ~/.bashrc
                echo -e "${GREEN}✓ 已添加别名，请执行: source ~/.bashrc${NC}"
            fi
        fi
    fi
}

# 显示客户端配置说明
show_client_config_instructions() {
    local new_socket="$1"
    
    echo ""
    echo -e "${YELLOW}客户端配置说明：${NC}"
    echo "================================================"
    echo "要使 'mysql' 命令直接连接到迁移后的实例，请手动添加以下配置："
    echo ""
    
    echo "1. 创建或编辑系统级配置文件："
    echo "   sudo vi /etc/my.cnf.d/mysql-client.cnf"
    echo "   添加以下内容："
    echo "   [client]"
    echo "   socket = $new_socket"
    echo ""
    echo "   [mysql]"
    echo "   socket = $new_socket"
    echo ""
    
    echo "2. 或者创建用户级配置文件："
    echo "   vi ~/.my.cnf"
    echo "   添加相同内容"
    echo ""
    
    echo "3. 或者设置环境变量："
    echo "   添加到 ~/.bashrc："
    echo "   export MYSQL_UNIX_PORT=\"$new_socket\""
    echo ""
    
    echo "4. 快速测试（不保存配置）："
    echo "   MYSQL_UNIX_PORT=\"$new_socket\" mysql -e \"SELECT 1;\""
    echo ""
    
    echo "5. 使用别名（推荐给多实例环境）："
    echo "   在 ~/.bashrc 中添加："
    echo "   alias mysql-migrated=\"mysql -S '$new_socket'\""
    echo ""
    
    echo -e "${RED}注意：如果系统中有多个MySQL实例，可能需要使用不同配置或别名来区分。${NC}"
}

validate_datadir_match() {
    local expected="$1"
    local actual="$2"
    
    # 规范化路径（移除末尾斜杠）
    expected=$(echo "$expected" | sed 's:/*$::')
    actual=$(echo "$actual" | sed 's:/*$::')
    
    # 使用 realpath 解析符号链接（如果可用）
    if command -v realpath &>/dev/null; then
        expected=$(realpath "$expected" 2>/dev/null || echo "$expected")
        actual=$(realpath "$actual" 2>/dev/null || echo "$actual")
    fi
    
    if [[ "$actual" == "$expected" ]]; then
        return 0  # 匹配
    else
        return 1  # 不匹配
    fi
}

verify_migration_MB() {
    local new_socket=""
    local old_datadir=${MYSQL_INSTANCES["${CURRENT_INSTANCE}_datadir"]}
    local old_socket=${MYSQL_INSTANCES["${CURRENT_INSTANCE}_socket"]}
    local expected_dir="$NEW_DATA_DIR"
    local validation_passed=true  
    
    if [[ "$old_socket" == "$old_datadir/"* ]]; then
        new_socket="${NEW_DATA_DIR}${old_socket#$old_datadir}"
    else
        new_socket="$old_socket"
    fi
    
    echo ""
    echo -e "${GREEN}迁移验证：${NC}"
    echo_quote
    # echo "================================================"
    echo "使用验证系统版本: 1.0"
    echo "查询文件哈希: $VALIDATION_QUERIES_HASH"
    echo_quote
    # echo ""
    
    # 1. 服务状态检查
    local service=${MYSQL_INSTANCES["${CURRENT_INSTANCE}_service"]}
    
    echo "1. 服务状态检查..."
    if sudo systemctl is-active "$service" &>/dev/null; then
        echo -e "  ${GREEN}✓ 服务正在运行${NC}"
    else
        echo -e "  ${RED}✗ 服务未运行${NC}"
        # 尝试启动服务
        echo "尝试启动 MySQL 服务..."
        if sudo systemctl start "$service"; then
            echo -e "  ${GREEN}✓ 服务启动成功${NC}"
        else
            echo -e "  ${RED}✗ 服务启动失败${NC}"
            sudo journalctl -u "$service" --since "5 minutes ago" | tail -20
            return 1
        fi
    fi
    
    # 2. 等待服务就绪
    echo "2. 等待服务就绪..."
    for i in {1..90}; do
        if sudo mysql -S "$new_socket" -e "SELECT 1;" &>/dev/null; then
            echo -e "  ${GREEN}✓ 服务已就绪 ($i 秒)${NC}"
            break
        fi
        sleep 1
        if [[ $i -eq 30 ]]; then
            echo -e "  ${RED}✗ 服务启动超时${NC}"
            return 1
        fi
    done

    # 创建查询管理器
    QueryManager "$new_socket"
    
    # 3. 选择验证级别
    echo "请选择验证级别："
    echo "1. 快速验证"
    echo "2. 标准验证"
    echo -e "${RED}3. 详细验证（推荐）${NC}"
    echo "4. 自定义验证（选择查询）"
    echo "5. 跳过验证"
    echo ""
    
    local choice=0
    read -p "选择 (1-5): " choice
    
    case $choice in
        1)
            validate_database_quick_table "$new_socket" "$NEW_DATA_DIR"
            validation_passed=$?
            ;;
        2)
            validate_database_standard "$new_socket" "$NEW_DATA_DIR"
            validation_passed=$?
            ;;
        3)
            echo_quote
            validate_database_extended "$new_socket" "$NEW_DATA_DIR"
            validation_passed=$?
            echo_quote
            ;;
        4)
            validate_database_selective "$new_socket" "$NEW_DATA_DIR"
            validation_passed=$?
            ;;
        5)
            echo "跳过验证"
            validation_passed=0
            ;;
        *)
            echo "无效选择，跳过验证"
            validation_passed=0
            ;;
    esac
    
    # 4. 关键验证：数据目录（必须执行）
    echo ""
    echo  "4. 关键指标验证："
    
    # 注意：这里的 NEW_DATA_DIR 需要已经在之前的步骤中设置
    if validate_datadir "$new_socket" "$NEW_DATA_DIR"; then
        echo -e "  ${GREEN}✓ 数据目录验证成功${NC}"
    else
        echo -e "  ${RED}✗ 数据目录验证失败${NC}"
        validation_passed=false
    fi
    
    # 5. 生成报告（可选）
    if [[ "$validation_passed" -eq 0 ]]; then
        echo ""
        read -p "是否生成详细验证报告? (yes/no): " generate_report
        
        if [[ "$generate_report" =~ ^(yes|YES|y|Y)$ ]]; then
            local report_file=$(generate_validation_report "$new_socket")
            echo "验证报告: $report_file"
        fi
        
        echo ""
        echo -e "${GREEN}✓ 迁移验证完成！${NC}"
        return 0
    else
        echo ""
        echo -e "${YELLOW}⚠ 验证发现问题，但迁移可能已完成${NC}"
        echo "请检查以下内容："
        echo "  1. 服务状态: sudo systemctl status $service"
        echo "  2. 错误日志: sudo journalctl -u $service --since '5 minutes ago'"
        echo "  3. 手动验证: sudo mysql -S $new_socket"
        return 1
    fi
}

# 生成清理命令和报告
generate_report() {
    local service=${MYSQL_INSTANCES["${CURRENT_INSTANCE}_service"]}
    local datadir=${MYSQL_INSTANCES["${CURRENT_INSTANCE}_datadir"]}
    
    echo ""
    echo -e "${GREEN}迁移完成！${NC}"
    echo_quote
    # echo "================================================"
    echo "迁移时间: $(date)"
    echo "原数据目录: $datadir"
    echo "新数据目录: $NEW_DATA_DIR"
    echo "影响的服务: $service"
    echo_quote
    echo "系统当前磁盘使用情况："
    lsblk -af
    echo_quote
    # echo ""
    
    echo -e "接下来请${RED}自行${NC}执行："
    echo "清理旧数据（确认新实例运行正常后）："
    echo "   # 首先备份旧数据（可选）"
    echo "   sudo mv \"$datadir\" \"${datadir}_backup_$(date +%Y%m%d_%H%M%S)\""
    echo ""
    echo "   # 运行一段时间确认无误后，再删除备份"
    echo "   sudo rm -rf \"${datadir}_backup_$(date +%Y%m%d_%H%M%S)\""
    echo ""
    echo -e "${RED}警告：删除旧数据前请确保新实例运行正常！${NC}"
    echo_quote
    echo "迁移日志: $MIGRATION_LOG"
    echo_quote
    # echo "================================================"
}

# 清理无效配置文件（需要用户确认）
cleanup_unused_configs() {
    echo_quote
    echo -e "${RED}⚠ 配置文件清理工具 ⚠${NC}"
    echo_quote
    echo "此功能将帮助清理无效的MySQL配置文件。"
    echo "注意：操作前会自动创建备份。"
    echo ""
    
    read -p "是否继续? (yes/no): " proceed_cleanup
    if [[ ! "$proceed_cleanup" =~ ^(yes|YES|y|Y)$ ]]; then
        echo "用户取消清理操作"
        return 0
    fi
    
    # 创建清理日志
    local cleanup_log="/tmp/mysql_config_cleanup_$(date +%Y%m%d_%H%M%S).log"
    local backup_dir="/tmp/mysql_config_backup_$(date +%Y%m%d_%H%M%S)"
    
    sudo mkdir -p "$backup_dir"
    echo "=== MySQL配置文件清理日志 $(date) ===" > "$cleanup_log"
    echo "备份目录: $backup_dir" >> "$cleanup_log"
    
    # 1. 检查重复的systemd服务配置
    echo ""
    echo "1. 检查重复的systemd服务配置..."
    
    declare -A datadir_service_map
    declare -A duplicate_services
    
    # 建立数据目录到服务的映射
    for name in "${INSTANCE_NAMES[@]}"; do
        local datadir=${MYSQL_INSTANCES["${name}_datadir"]}
        local service=${MYSQL_INSTANCES["${name}_service"]}
        local service_file=${MYSQL_INSTANCES["${instance_name}_service_file_path"]:-}
        local status=${MYSQL_INSTANCES["${name}_status"]}
        
        if [[ "$datadir" != "<unknown>" ]] && [[ -n "$datadir" ]] && [[ -n "$service_file" ]]; then
            if [[ -n "${datadir_service_map[$datadir]}" ]]; then
                duplicate_services["$datadir"]="${datadir_service_map[$datadir]},$service:$status"
            else
                datadir_service_map["$datadir"]="$service:$status"
            fi
        fi
    done
    
    # 显示重复配置
    if [[ ${#duplicate_services[@]} -gt 0 ]]; then
        echo -e "${YELLOW}发现重复配置:${NC}"
        for datadir in "${!duplicate_services[@]}"; do
            echo "  数据目录: $datadir"
            IFS=',' read -ra services <<< "${duplicate_services[$datadir]}"
            for svc_info in "${services[@]}"; do
                local svc="${svc_info%:*}"
                local stat="${svc_info#*:}"
                echo "    - $svc (状态: $stat)"
            done
        done
        
        echo ""
        echo -e "${RED}建议操作:${NC}"
        echo "  对于每个数据目录，保留一个活跃的服务，禁用其他服务。"
        echo ""
        
        read -p "是否显示清理命令? (yes/no): " show_commands
        if [[ "$show_commands" =~ ^(yes|YES|y|Y)$ ]]; then
            for datadir in "${!duplicate_services[@]}"; do
                echo ""
                echo "数据目录: $datadir"
                IFS=',' read -ra services <<< "${duplicate_services[$datadir]}"
                
                local active_found=false
                for svc_info in "${services[@]}"; do
                    local svc="${svc_info%:*}"
                    local stat="${svc_info#*:}"
                    local service_file=$(systemctl show "$svc" --property=FragmentPath --value 2>/dev/null)
                    
                    if [[ "$stat" == "active" ]] && [[ "$active_found" == false ]]; then
                        echo "  # 保留活跃服务: $svc"
                        active_found=true
                    else
                        echo "  # 禁用重复服务: $svc"
                        echo "  sudo systemctl disable $svc"
                        echo "  sudo systemctl stop $svc"
                        
                        if [[ -f "$service_file" ]]; then
                            echo "  # 备份配置文件:"
                            echo "  sudo cp \"$service_file\" \"$backup_dir/$(basename "$service_file").backup\""
                            echo "  # 可选: 重命名配置文件"
                            echo "  sudo mv \"$service_file\" \"${service_file}.disabled\""
                        fi
                    fi
                done
            done
        fi
    else
        echo "未发现重复的systemd服务配置"
    fi
    
    # 2. 检查无效的配置文件
    echo ""
    echo "2. 检查无效的MySQL配置文件..."
    
    # 查找所有可能配置文件
    local config_dirs=(
        "/etc/my.cnf"
        "/etc/my.cnf.d/"
        "/etc/mysql/"
        "/usr/local/etc/my.cnf"
    )
    
    local unused_configs=()
    
    for config_dir in "${config_dirs[@]}"; do
        if [[ -e "$config_dir" ]]; then
            if [[ -d "$config_dir" ]]; then
                local config_files=$(find "$config_dir" -name "*.cnf" -type f 2>/dev/null)
            else
                local config_files="$config_dir"
            fi
            
            for config_file in $config_files; do
                # 检查配置文件是否被任何实例使用
                local config_in_use=false
                
                for name in "${INSTANCE_NAMES[@]}"; do
                    local conf_files_str=${MYSQL_INSTANCES_CONF_FILES["${name}"]:-}
                    if [[ "$conf_files_str" =~ $config_file ]]; then
                        config_in_use=true
                        break
                    fi
                done
                
                if [[ "$config_in_use" == false ]] && [[ -f "$config_file" ]]; then
                    unused_configs+=("$config_file")
                    echo "  ⚠ 可能未使用的配置文件: $config_file"
                fi
            done
        fi
    done
    
    if [[ ${#unused_configs[@]} -gt 0 ]]; then
        echo ""
        echo "建议备份这些配置文件后，根据需要删除或归档。"
        
        read -p "是否生成清理命令? (yes/no): " gen_clean_commands
        if [[ "$gen_clean_commands" =~ ^(yes|YES|y|Y)$ ]]; then
            echo ""
            echo "# 备份和清理命令:"
            for config in "${unused_configs[@]}"; do
                echo "sudo cp \"$config\" \"$backup_dir/$(basename "$config").backup\""
                echo "# sudo rm \"$config\"  # 谨慎: 先备份后删除"
            done
        fi
    else
        echo "未发现明显未使用的配置文件"
    fi
    
    # 保存清理报告
    echo ""
    echo "清理日志已保存: $cleanup_log"
    echo "备份文件目录: $backup_dir"
    echo ""
    echo -e "${GREEN}清理工具执行完成${NC}"
    echo "建议："
    echo "  1. 检查备份目录中的文件"
    echo "  2. 验证现有服务功能正常"
    echo "  3. 再考虑删除备份文件"
    
    echo_quote
}

# 主函数
main() {
    # 检查参数
    check_args "$@"

    # 显示目录信息
    show_directory_info

    # 加载验证库
    load_validation_system

    # 确认风险
    if [[ $LIST_ONLY == "false" ]]; then
        show_banner_ascii
        confirm_risk
    fi
    
    # 检测实例
    detect_mysql_instances

    # 分析所有实例的配置来源
    echo_quote
    echo -e "${GREEN}分析所有实例配置来源...${NC}"
    echo_quote
    
    for name in "${INSTANCE_NAMES[@]}"; do
        analyze_configuration_sources "$name"
    done
    
    # # 选择实例
    # select_instance
    
    # 选择实例（使用增强版）
    select_instance_enhanced

    # 收集选定实例的历史信息
    collect_instance_history "$CURRENT_INSTANCE"

    # 显示操作命令
    show_operations
    
    # 确认备份状态
    confirm_backup
    
    # 确认服务已停止
    confirm_service_stopped
    
    # 选择新目录
    select_new_directory
    
    # 检查目录是否是同一目录
    check_data_directory_conflicts

    # 执行数据拷贝
    execute_data_copy

    # 在迁移完成后调用
    fix_permissions "$NEW_DATA_DIR"
    
    # 更新配置文件
    update_configurations

    # 验证迁移结果
    verify_migration_MB
    # if verify_migration_V2; then
    #     generate_report
    # else
    #     echo -e "${RED}迁移验证失败！请检查日志：$MIGRATION_LOG${NC}"
    #     exit 1
    # fi

    # 显示报告
    generate_report

    # 结束脚本
    end_the_batch

    log "INFO" "脚本执行完成"
    log "INFO" "详细日志: $MIGRATION_LOG"
    echo_quote

    # 在迁移完成后，询问是否清理无效配置
    echo ""
    read -p "迁移完成，是否检查并清理无效配置文件? (yes/no): " cleanup_choice
    if [[ "$cleanup_choice" =~ ^(yes|YES|y|Y)$ ]]; then
        cleanup_unused_configs
    fi

}

end_the_batch() {
    echo ""
    echo "================================================"
    echo -e "${GREEN}脚本执行结束。${NC}"
    echo "详细日志: $MIGRATION_LOG"
    echo "================================================"
    exit 0
}

# 运行主函数
main "$@"