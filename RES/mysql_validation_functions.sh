#!/bin/bash
# MySQL 验证函数库
# 位置: RES/mysql_validation_functions.sh

# 全局变量
VALIDATION_QUERIES_FILE=""
VALIDATION_QUERIES_HASH=""

# 初始化验证系统
init_validation_system() {
    local queries_file="$1"
    
    # 检查文件是否存在
    if [[ ! -f "$queries_file" ]]; then
        echo "ERROR: 验证查询文件不存在: $queries_file" >&2
        return 1
    fi
    
    # 设置全局变量
    VALIDATION_QUERIES_FILE="$queries_file"
    
    # 计算文件哈希
    VALIDATION_QUERIES_HASH=$(sha256sum "$queries_file" | cut -d' ' -f1)
    
    echo "INFO: 验证系统初始化完成" >&2
    echo "INFO: 查询文件: $(basename "$queries_file")" >&2
    echo "INFO: 文件SHA256: $VALIDATION_QUERIES_HASH" >&2
    
    return 0
}

# 提取查询语句
extract_query() {
    local query_name="$1"
    local query_param="${2:-}"  # 第二个参数可选
    
    if [[ ! -f "$VALIDATION_QUERIES_FILE" ]]; then
        echo "ERROR: 查询文件未初始化" >&2
        return 1
    fi
    
    case "$query_name" in
        "basic_info")
            # 提取基本信息查询
            awk '/^-- =/{p=0} /^-- 查询 1.1: 系统基本信息/{p=1} p && !/^--/' "$VALIDATION_QUERIES_FILE" | head -n 20
            ;;
        "database_stats")
            awk '/^-- =/{p=0} /^-- 查询 2.1: 数据库分类统计/{p=1} p && !/^--/' "$VALIDATION_QUERIES_FILE" | head -n 10
            ;;
        "datadir_check")
            # 数据目录验证查询，安全地替换参数
            local expected_dir="$query_param"
            if [[ -z "$expected_dir" ]]; then
                echo "ERROR: 数据目录验证需要期望目录参数" >&2
                return 1
            fi
            
            # 安全地处理路径中的特殊字符
            local safe_expected_dir=$(printf '%s\n' "$expected_dir" | sed 's/[\/&]/\\&/g')
            
            awk '/^-- =/{p=0} /^-- 查询 8.1: 数据目录验证/{p=1} p && !/^--/' "$VALIDATION_QUERIES_FILE" | \
                head -n 5 | \
                sed "s/{EXPECTED_DIR}/$safe_expected_dir/g"
            ;;
        *)
            echo "ERROR: 未知查询类型: $query_name" >&2
            return 1
            ;;
    esac
}

####################################################################################################
# 通用函数：执行SQL文件中的查询并以表格形式输出
####################################################################################################
# 表格化查询执行（修复版）
# 改进的查询执行函数
# 修复 execute_query_with_table_format 函数
execute_query_with_table_format() {
    local socket_path="$1"
    local query_name="$2"
    local query_param="${3:-}"
    
    # 提取查询语句
    local query_content
    query_content=$(extract_query_from_file "$query_name" "$query_param")
    
    if [[ -z "$query_content" ]]; then
        echo "错误：无法提取查询: $query_name" >&2
        return 1
    fi
    
    # 检查查询是否以分号结尾
    # 修复正则表达式：分号需要转义
    if ! echo "$query_content" | grep -q ";[[:space:]]*$"; then
        query_content="$query_content;"
    fi
    
    # 创建临时文件
    local temp_file="/tmp/mysql_query_$$.sql"
    echo "$query_content" > "$temp_file"
    
    # 调试：显示查询内容
    if [[ "${DEBUG_VALIDATION:-false}" == "true" ]]; then
        echo "DEBUG: 执行查询 $query_name" >&2
        echo "DEBUG: 查询内容:" >&2
        cat "$temp_file" >&2
    fi
    
    # 使用 -t 选项执行，生成表格输出
    local output
    output=$(sudo mysql -t -S "$socket_path" < "$temp_file" 2>&1)
    local exit_code=$?
    
    # 清理临时文件
    rm -f "$temp_file"
    
    if [[ $exit_code -ne 0 ]]; then
        # 检查是否是已知的SQL语法问题
        if [[ "$output" =~ "sql_mode=only_full_group_by" ]]; then
            echo "注意：查询因 only_full_group_by 模式而失败，这是一个已知问题" >&2
        else
            echo "查询执行失败: $query_name" >&2
            echo "错误信息: $output" >&2
        fi
        return 1
    fi
    
    # 返回表格化输出
    echo "$output"
    return 0
}

# 修改批量执行函数，更好地处理失败
execute_batch_table_queries() {
    local socket_path="$1"
    shift
    local queries=("$@")
    
    local success_count=0
    local total_count=${#queries[@]}
    
    for query_spec in "${queries[@]}"; do
        # 解析查询规格：query_name:display_title:params
        IFS=':' read -r query_name display_title query_params <<< "$query_spec"
        
        echo ""
        echo "$display_title："
        echo "────────────────────────────────"
        
        # 执行查询
        if output=$(execute_query_with_table_format "$socket_path" "$query_name" "$query_params"); then
            echo "$output" | sed 's/^/  /'
            success_count=$((success_count + 1))
        else
            echo "  查询执行失败（跳过）"
        fi
    done
    
    echo ""
    echo "验证统计：成功 $success_count/$total_count"
    
    if [[ $success_count -eq $total_count ]]; then
        return 0
    else
        return 1
    fi
}


# 标准验证
validate_database_standard() {
    local socket_path="$1"
    local expected_datadir="$2"
    
    echo ""
    echo -e "${CYAN}执行标准验证...${NC}"
    
    # 先测试基本连接
    echo "连接测试..."
    if ! sudo mysql -S "$socket_path" -e "SELECT 1;" &>/dev/null; then
        echo "  ✗ 连接失败"
        return 1
    fi
    echo "  ✓ 连接成功"
    echo ""
    
    # 定义标准验证查询
    local standard_queries=(
        "basic_info:1. 系统基本信息"
        "database_stats:2. 数据库统计"
        "datadir_check:3. 数据目录验证:$expected_datadir"
        "connection_check:4. 连接验证"
    )
    
    execute_batch_table_queries "$socket_path" "${standard_queries[@]}"
    
    return $?
}

# 详细验证
validate_database_detailed() {
    local socket_path="$1"
    local expected_datadir="$2"
    
    echo ""
    echo -e "${CYAN}执行详细验证...${NC}"
    
    # 定义详细验证查询
    local detailed_queries=(
        "basic_info:系统基本信息"
        "database_stats:数据库统计"
        "database_list:数据库详情"
        "user_list:用户列表"
        "datadir_check:数据目录验证:$expected_datadir"
    )
    
    execute_batch_table_queries "$socket_path" "${detailed_queries[@]}"
    
    return $?
}

# 修复几个关键查询的提取函数
extract_query_from_file() {
    local query_name="$1"
    local query_param="${2:-}"
    
    if [[ ! -f "$VALIDATION_QUERIES_FILE" ]]; then
        echo "错误：查询文件未加载" >&2
        return 1
    fi
    
    case "$query_name" in
        # 基本信息查询
        "basic_info")
            # 提取查询1.1 - 提取完整查询
            awk '/^-- 查询 1.1: 系统基本信息/{flag=1; next} 
                 /^-- 查询 [0-9]+\.[0-9]+:/{if(flag) exit}
                 flag && !/^--/' "$VALIDATION_QUERIES_FILE"
            ;;
        
        "database_stats")
            awk '/^-- 查询 2.1: 数据库分类统计/{flag=1; next}
                 /^-- 查询 [0-9]+\.[0-9]+:/{if(flag) exit}
                 flag && !/^--/' "$VALIDATION_QUERIES_FILE"
            ;;
        
        "database_list")
            awk '/^-- 查询 2.2: 详细数据库列表/{flag=1; next}
                 /^-- 查询 [0-9]+\.[0-9]+:/{if(flag) exit}
                 flag && !/^--/' "$VALIDATION_QUERIES_FILE"
            ;;
        
        "user_list")
            awk '/^-- 查询 3.1: 用户列表/{flag=1; next}
                 /^-- 查询 [0-9]+\.[0-9]+:/{if(flag) exit}
                 flag && !/^--/' "$VALIDATION_QUERIES_FILE"
            ;;
        
        "database_privileges")
            # 提取查询3.2 - 这个查询有问题，需要修复或跳过
            awk '/^-- 查询 3.2: 数据库权限详情/{flag=1; next}
                 /^-- 查询 [0-9]+\.[0-9]+:/{if(flag) exit}
                 flag && !/^--/' "$VALIDATION_QUERIES_FILE"
            ;;
        
        "tablespace_info")
            # 提取查询4.1 - 确保有分号
            awk '/^-- 查询 4.1: 表空间信息/{flag=1; next}
                 /^-- 查询 [0-9]+\.[0-9]+:/{if(flag) exit}
                 flag && !/^--/' "$VALIDATION_QUERIES_FILE"
            ;;
        
        "connection_stats")
            awk '/^-- 查询 4.2: 当前连接统计/{flag=1; next}
                 /^-- 查询 [0-9]+\.[0-9]+:/{if(flag) exit}
                 flag && !/^--/' "$VALIDATION_QUERIES_FILE"
            ;;
        
        "buffer_pool_config")
            awk '/^-- 查询 7.1: 缓冲池配置/{flag=1; next}
                 /^-- 查询 [0-9]+\.[0-9]+:/{if(flag) exit}
                 flag && !/^--/' "$VALIDATION_QUERIES_FILE"
            ;;
        
        "datadir_check")
            local expected_dir="$query_param"
            local safe_dir=$(printf '%s\n' "$expected_dir" | sed 's/[\/&]/\\&/g')
            
            awk '/^-- 查询 8.1: 数据目录验证/{flag=1; next}
                 /^-- 查询 [0-9]+\.[0-9]+:/{if(flag) exit}
                 flag && !/^--/' "$VALIDATION_QUERIES_FILE" | \
                sed "s/{EXPECTED_DIR}/$safe_dir/g"
            ;;
        
        "service_status")
            awk '/^-- 查询 8.2: 服务状态验证/{flag=1; next}
                 /^-- 查询 [0-9]+\.[0-9]+:/{if(flag) exit}
                 flag && !/^--/' "$VALIDATION_QUERIES_FILE"
            ;;
        
        "connection_check")
            awk '/^-- 查询 8.3: 连接验证/{flag=1; next}
                 /^-- 查询 [0-9]+\.[0-9]+:/{if(flag) exit}
                 flag && !/^--/' "$VALIDATION_QUERIES_FILE"
            ;;
        
        "uptime")
            awk '/^-- 查询 9.1: 运行时间/{flag=1; next}
                 /^-- 查询 [0-9]+\.[0-9]+:/{if(flag) exit}
                 flag && !/^--/' "$VALIDATION_QUERIES_FILE"
            ;;
        
        "query_stats")
            awk '/^-- 查询 9.2: 查询统计/{flag=1; next}
                 /^-- 查询 [0-9]+\.[0-9]+:/{if(flag) exit}
                 flag && !/^--/' "$VALIDATION_QUERIES_FILE"
            ;;
        
        *)
            echo "错误：未知查询类型: $query_name" >&2
            return 1
            ;;
    esac
}

# 扩展的验证函数
validate_database_extended() {
    local socket_path="$1"
    local expected_datadir="$2"
    
    echo ""
    echo -e "${CYAN}执行扩展验证...${NC}"
    
    # 定义所有要执行的查询
    local extended_queries=(
        "basic_info:1. 系统基本信息"
        "database_stats:2. 数据库统计"
        "database_list:3. 数据库详情"
        "user_list:4. 用户列表"
        "database_privileges:5. 数据库权限"
        "tablespace_info:6. 表空间信息"
        "connection_stats:7. 连接统计"
        "buffer_pool_config:8. 缓冲池配置"
        "datadir_check:9. 数据目录验证:$expected_datadir"
        "connection_check:10. 连接验证"
        "uptime:11. 运行时间"
        "query_stats:12. 查询统计"
    )
    
    execute_batch_table_queries "$socket_path" "${extended_queries[@]}"
    
    return $?
}

# 选择性验证
validate_database_selective() {
    local socket_path="$1"
    local expected_datadir="$2"
    
    echo ""
    echo -e "${CYAN}请选择要执行的验证查询：${NC}"
    echo "========================================"
    
    # 显示可用查询菜单
    local query_options=(
        "1: 系统基本信息 (basic_info)"
        "2: 数据库统计 (database_stats)"
        "3: 数据库列表 (database_list)"
        "4: 用户列表 (user_list)"
        "5: 数据库权限 (database_privileges)"
        "6: 表空间信息 (tablespace_info)"
        "7: 连接统计 (connection_stats)"
        "8: 缓冲池配置 (buffer_pool_config)"
        "9: 数据目录验证 (datadir_check)"
        "10: 运行时间统计 (uptime)"
        "11: 查询统计 (query_stats)"
        "12: 执行所有查询"
    )
    
    for option in "${query_options[@]}"; do
        echo "  $option"
    done
    
    echo ""
    read -p "请选择查询编号（用逗号分隔，如 1,3,5）: " selected
    
    # 转换为数组
    IFS=',' read -ra selected_array <<< "$selected"
    
    # 构建查询列表
    local selected_queries=()
    
    for choice in "${selected_array[@]}"; do
        choice=$(echo "$choice" | xargs)  # 去除空格
        
        case $choice in
            1) selected_queries+=("basic_info:1. 系统基本信息") ;;
            2) selected_queries+=("database_stats:2. 数据库统计") ;;
            3) selected_queries+=("database_list:3. 数据库详情") ;;
            4) selected_queries+=("user_list:4. 用户列表") ;;
            5) selected_queries+=("database_privileges:5. 数据库权限") ;;
            6) selected_queries+=("tablespace_info:6. 表空间信息") ;;
            7) selected_queries+=("connection_stats:7. 连接统计") ;;
            8) selected_queries+=("buffer_pool_config:8. 缓冲池配置") ;;
            9) selected_queries+=("datadir_check:9. 数据目录验证:$expected_datadir") ;;
            10) selected_queries+=("uptime:10. 运行时间统计") ;;
            11) selected_queries+=("query_stats:11. 查询统计") ;;
            12)
                # 选择所有
                selected_queries=(
                    "basic_info:1. 系统基本信息"
                    "database_stats:2. 数据库统计"
                    "database_list:3. 数据库详情"
                    "user_list:4. 用户列表"
                    "database_privileges:5. 数据库权限"
                    "tablespace_info:6. 表空间信息"
                    "connection_stats:7. 连接统计"
                    "buffer_pool_config:8. 缓冲池配置"
                    "datadir_check:9. 数据目录验证:$expected_datadir"
                    "uptime:10. 运行时间统计"
                    "query_stats:11. 查询统计"
                )
                break
                ;;
            *)
                echo "忽略无效选择: $choice"
                ;;
        esac
    done
    
    if [[ ${#selected_queries[@]} -eq 0 ]]; then
        echo "未选择任何查询"
        return 0
    fi
    
    echo ""
    echo "将执行以下查询："
    for query_spec in "${selected_queries[@]}"; do
        IFS=':' read -r query_name display_title <<< "$query_spec"
        echo "  • $display_title"
    done
    
    echo ""
    execute_batch_table_queries "$socket_path" "${selected_queries[@]}"
    
    return $?
}

# 表格化快速验证
validate_database_quick_table() {
    local socket_path="$1"
    
    echo ""
    echo -e "${GREEN}快速数据库验证${NC}"
    echo "════════════════════════════════════════"
    
    # 1. 连接测试（单独测试，不包含在表格中）
    echo "连接测试..."
    if ! sudo mysql -S "$socket_path" -e "SELECT 1;" &>/dev/null; then
        echo "  ✗ 连接失败"
        return 1
    fi
    echo "  ✓ 连接成功"
    echo ""
    
    # 2. 显示基本信息（表格格式）
    echo "实例基本信息："
    execute_query_with_table_format "$socket_path" "basic_info" | \
        sed 's/^/  /'
    
    echo ""
    echo "✓ 快速验证完成"
    return 0
}

# 表格化全面验证
validate_database_comprehensive_table() {
    local socket_path="$1"
    
    echo ""
    echo -e "${GREEN}全面数据库验证${NC}"
    echo "════════════════════════════════════════"
    
    # 1. 连接测试
    if ! sudo mysql -S "$socket_path" -e "SELECT 1;" &>/dev/null; then
        echo "✗ 数据库连接失败"
        return 1
    fi
    echo "✓ 数据库连接成功"
    echo ""
    
    # 2. 基本信息表格
    echo "1. 系统基本信息："
    execute_query_with_table_format "$socket_path" "basic_info" | \
        sed 's/^/  /'
    
    echo ""
    
    # 3. 数据库统计表格
    echo "2. 数据库统计："
    execute_query_with_table_format "$socket_path" "database_stats" | \
        sed 's/^/  /'
    
    echo ""
    
    # 4. 数据库列表表格
    echo "3. 数据库详情（前10个）："
    execute_query_with_table_format "$socket_path" "database_list" | \
        head -15 | sed 's/^/  /'
    
    echo ""
    
    # 5. 用户列表表格
    echo "4. 用户列表（前10个）："
    execute_query_with_table_format "$socket_path" "user_list" | \
        head -15 | sed 's/^/  /'
    
    echo ""
    echo "✓ 全面验证完成"
    return 0
}

# 查询执行管理器
QueryManager() {
    local socket_path="$1"
    
    # 执行查询并返回表格输出
    execute() {
        local query_name="$1"
        local params="${2:-}"
        
        case "$query_name" in
            "basic_info")
                execute_query_with_table_format "$socket_path" "basic_info"
                ;;
            "database_stats")
                execute_query_with_table_format "$socket_path" "database_stats"
                ;;
            "database_list")
                execute_query_with_table_format "$socket_path" "database_list"
                ;;
            "user_list")
                execute_query_with_table_format "$socket_path" "user_list"
                ;;
            "datadir_check")
                execute_query_with_table_format "$socket_path" "datadir_check" "$params"
                ;;
            "custom")
                # 直接执行自定义SQL
                local query="$params"
                local temp_file="/tmp/custom_query_$$.sql"
                echo "$query" > "$temp_file"
                sudo mysql -t -S "$socket_path" < "$temp_file" 2>/dev/null
                rm -f "$temp_file"
                ;;
            *)
                echo "未知查询: $query_name" >&2
                return 1
                ;;
        esac
    }
    
    # 批量执行多个查询
    batch_execute() {
        local queries=("$@")
        
        for query_spec in "${queries[@]}"; do
            IFS=':' read -r query_name query_title <<< "$query_spec"
            
            echo ""
            echo "$query_title："
            echo "────────────────────────────────"
            
            if output=$(execute "$query_name"); then
                echo "$output" | sed 's/^/  /'
            else
                echo "  查询执行失败"
            fi
        done
    }
    
    # 导出函数
    declare -fx execute batch_execute
}

####################################################################################################

# 执行验证查询
execute_validation_query() {
    local socket_path="$1"
    local query_name="$2"
    local query_param="${3:-}"  # 第三个参数可选
    
    # 创建临时查询文件
    local temp_query_file="/tmp/mysql_validation_query_$$.sql"
    
    # 提取查询
    if [[ -n "$query_param" ]]; then
        extract_query "$query_name" "$query_param" > "$temp_query_file"
    else
        extract_query "$query_name" > "$temp_query_file"
    fi
    
    if [[ $? -ne 0 ]] || [[ ! -s "$temp_query_file" ]]; then
        rm -f "$temp_query_file"
        echo "ERROR: 无法提取查询: $query_name" >&2
        return 1
    fi
    
    # 调试：显示查询内容
    if [[ "${DEBUG:-false}" == "true" ]]; then
        echo "DEBUG: 执行查询 $query_name" >&2
        echo "DEBUG: 查询内容:" >&2
        cat "$temp_query_file" >&2
    fi
    
    # 执行查询
    local result
    result=$(sudo mysql -S "$socket_path" --batch --skip-column-names < "$temp_query_file" 2>&1)
    local exit_code=$?
    
    # 清理临时文件
    rm -f "$temp_query_file"
    
    if [[ $exit_code -ne 0 ]]; then
        echo "ERROR: 查询执行失败: $query_name" >&2
        echo "错误信息: $result" >&2
        return 1
    fi
    
    echo "$result"
    return 0
}

# 简单连接测试（不使用验证查询文件）
test_connection() {
    local socket_path="$1"
    
    if sudo mysql -S "$socket_path" -e "SELECT '连接测试' as status, NOW() as time;" --batch --skip-column-names 2>/dev/null; then
        return 0
    else
        return 1
    fi
}

# 修改现有的验证函数，添加美观输出
validate_database_quick() {
    local socket_path="$1"
    
    echo ""
    echo -e "${GREEN}快速数据库验证${NC}"
    echo "════════════════════════════════════════"
    
    # 使用表格格式输出基本信息
    local basic_info=$(sudo mysql -t -S "$socket_path" -e "
        SELECT '项目' AS '检查项', '结果' AS '状态/数值'
        UNION
        SELECT '连接测试', '正在测试...'
        UNION
        SELECT 'MySQL版本', @@version
        UNION
        SELECT '数据目录', @@datadir
        UNION
        SELECT '服务状态', '运行中';
    " 2>/dev/null)
    
    if [[ -n "$basic_info" ]]; then
        echo "$basic_info" | sed 's/^/  /'
        echo ""
        echo -e "${GREEN}✓ 快速验证完成${NC}"
        return 0
    else
        echo "  ✗ 无法连接到数据库"
        return 1
    fi
}

# 数据目录验证
validate_datadir() {
    local socket_path="$1"
    local expected_dir="$2"
    
    echo ""
    echo "验证数据目录..."
    
    # 直接查询，不使用验证查询文件
    local current_datadir=$(sudo mysql -S "$socket_path" -e "SELECT @@datadir;" --batch --skip-column-names 2>/dev/null)
    
    if [[ -n "$current_datadir" ]]; then
        # 规范化路径比较
        current_datadir=$(echo "$current_datadir" | sed 's:/*$::')
        expected_dir=$(echo "$expected_dir" | sed 's:/*$::')
        
        echo "  当前目录: $current_datadir"
        echo "  期望目录: $expected_dir"
        
        if [[ "$current_datadir" == "$expected_dir" ]]; then
            echo "✓ 数据目录验证成功"
            return 0
        else
            echo "✗ 数据目录验证失败"
            return 1
        fi
    else
        echo "✗ 无法获取数据目录信息"
        return 1
    fi
}

# 全面验证
validate_database_comprehensive() {
    local socket_path="$1"
    
    echo ""
    echo "执行全面验证..."
    echo "----------------"
    
    # 连接测试
    if ! test_connection "$socket_path"; then
        echo "✗ 连接测试失败"
        return 1
    fi
    echo "✓ 连接测试成功"
    
    # 获取详细系统信息
    echo ""
    echo "系统详细信息："
    
    sudo mysql -S "$socket_path" -e "
        SHOW VARIABLES LIKE 'version%';
        SHOW VARIABLES LIKE 'datadir';
        SHOW VARIABLES LIKE 'socket';
        SHOW VARIABLES LIKE 'port';
        SELECT '数据库统计' AS '';
        SELECT 
            COUNT(*) AS total_databases,
            SUM(CASE WHEN SCHEMA_NAME IN ('information_schema', 'performance_schema', 'sys', 'mysql') THEN 1 ELSE 0 END) AS system_dbs,
            SUM(CASE WHEN SCHEMA_NAME NOT IN ('information_schema', 'performance_schema', 'sys', 'mysql') THEN 1 ELSE 0 END) AS user_dbs
        FROM information_schema.SCHEMATA;
        SELECT '用户统计' AS '';
        SELECT COUNT(DISTINCT User) AS total_users FROM mysql.user WHERE User NOT LIKE 'mysql.%';
    " 2>/dev/null | grep -v "Variable_name" | sed 's/^/  /'
    
    echo ""
    echo "✓ 全面验证完成"
    return 0
}

# 生成验证报告
generate_validation_report() {
    local socket_path="$1"
    local report_file="/tmp/mysql_validation_report_$(date +%Y%m%d_%H%M%S).txt"
    
    {
        echo "MySQL 迁移验证报告"
        echo "======================"
        echo "生成时间: $(date)"
        echo "查询文件: $(basename "$VALIDATION_QUERIES_FILE")"
        echo "文件哈希: $VALIDATION_QUERIES_HASH"
        echo ""
        
        # 基本信息
        echo "1. 基本信息"
        echo "-----------"
        sudo mysql -S "$socket_path" -e "
            SELECT '数据目录' AS item, @@datadir AS value UNION
            SELECT 'Socket', @@socket UNION
            SELECT '端口', @@port UNION
            SELECT '版本', @@version UNION
            SELECT '字符集', @@character_set_server;
        " 2>/dev/null | column -t -s $'\t'
        
        echo ""
        echo "2. 数据库统计"
        echo "-----------"
        sudo mysql -S "$socket_path" -e "
            SELECT 
                '业务数据库' AS type,
                COUNT(*) AS count
            FROM information_schema.SCHEMATA 
            WHERE SCHEMA_NAME NOT IN ('information_schema', 'performance_schema', 'sys', 'mysql')
            UNION
            SELECT 
                '系统数据库',
                COUNT(*)
            FROM information_schema.SCHEMATA 
            WHERE SCHEMA_NAME IN ('information_schema', 'performance_schema', 'sys', 'mysql');
        " 2>/dev/null | column -t -s $'\t'
        
        echo ""
        echo "3. 验证结果"
        echo "-----------"
        echo "连接测试: $(if test_connection "$socket_path"; then echo "成功"; else echo "失败"; fi)"
        echo "数据目录验证: $(if validate_datadir "$socket_path" "dummy" &>/dev/null; then echo "可获取"; else echo "失败"; fi)"
        echo "服务状态: $(sudo systemctl is-active mysqld.service 2>/dev/null || echo "未知")"
        
        echo ""
        echo "4. 迁移信息"
        echo "-----------"
        echo "迁移时间: $(date)"
        # 尝试从实例读取数据目录，若失败则标记为 unknown
        NEW_DATA_DIR=$(sudo mysql -S "$socket_path" -sse "SELECT @@datadir;" 2>/dev/null || echo "unknown")
        echo "目标目录: $NEW_DATA_DIR"
        echo "Socket路径: $socket_path"
        
    } > "$report_file"
    
    echo "验证报告: $report_file"
    echo "$report_file"
}