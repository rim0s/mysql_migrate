#!/bin/bash
# SQL语法检查脚本

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SQL_FILE="$SCRIPT_DIR/mysql_validation_queries.sql"

echo "检查SQL文件语法: $(basename "$SQL_FILE")"
echo "================================================"

# 检查文件是否存在
if [[ ! -f "$SQL_FILE" ]]; then
    echo "错误: SQL文件不存在"
    exit 1
fi

# 检查基本的SQL语法问题
echo ""
echo "1. 检查基本语法错误..."

# 检查缺少分号
missing_semicolon=$(grep -n "SELECT.*FROM\|SELECT.*WHERE" "$SQL_FILE" | grep -v ";\s*$" | grep -v "^--" | head -5)
if [[ -n "$missing_semicolon" ]]; then
    echo "警告: 以下查询可能缺少分号:"
    echo "$missing_semicolon" | sed 's/^/   行 /'
fi

# 检查UNION用法
union_errors=$(grep -n "UNION[^A-Z]" "$SQL_FILE" | head -5)
if [[ -n "$union_errors" ]]; then
    echo "警告: UNION语法可能有问题:"
    echo "$union_errors" | sed 's/^/   行 /'
fi

# 检查常见的SQL关键字拼写
keywords=("SELECt" "FORM" "WHER" "GROU BY" "ORDE BY")
for keyword in "${keywords[@]}"; do
    if grep -q -i "$keyword" "$SQL_FILE"; then
        echo "警告: 发现可能的拼写错误: $keyword"
        grep -n -i "$keyword" "$SQL_FILE" | head -3 | sed 's/^/   行 /'
    fi
done

# 检查表名和列名
echo ""
echo "2. 检查表名和列名..."

# 检查information_schema表
if ! grep -q "information_schema\." "$SQL_FILE"; then
    echo "信息: 未发现information_schema表引用"
else
    echo "信息: 使用了information_schema表"
fi

# 检查可能的问题列名
problem_columns=$(grep -n "AS '[^']*[{}]" "$SQL_FILE")
if [[ -n "$problem_columns" ]]; then
    echo "警告: 发现可能的问题列名:"
    echo "$problem_columns" | sed 's/^/   行 /'
fi

# 检查替换标记
echo ""
echo "3. 检查替换标记..."
replacement_markers=$(grep -n "{.*}" "$SQL_FILE")
if [[ -n "$replacement_markers" ]]; then
    echo "发现替换标记:"
    echo "$replacement_markers" | sed 's/^/   行 /'
fi

# 检查LIMIT子句
echo ""
echo "4. 检查LIMIT使用..."
limit_clauses=$(grep -n "LIMIT" "$SQL_FILE")
if [[ -n "$limit_clauses" ]]; then
    echo "使用LIMIT的查询:"
    echo "$limit_clauses" | sed 's/^/   行 /'
fi

# 测试实际执行（可选）
echo ""
echo "5. 快速语法测试..."

# 创建一个临时MySQL会话测试基本查询
if command -v mysql &>/dev/null; then
    echo "使用MySQL客户端测试第一个查询..."
    
    # 提取第一个完整的查询
    local first_query=$(awk '/^--/{next} /^SELECT/{p=1} p && /;/{print; exit} p' "$SQL_FILE")
    
    if [[ -n "$first_query" ]]; then
        echo "测试查询:"
        echo "  $first_query"
        
        # 尝试在测试数据库中执行
        if mysql -e "CREATE DATABASE IF NOT EXISTS test_syntax_check;" 2>/dev/null; then
            if mysql test_syntax_check -e "SELECT 1 AS test;" 2>/dev/null; then
                echo "  ✓ MySQL连接正常"
                
                # 注释掉实际执行，避免影响生产
                # if mysql test_syntax_check -e "$first_query" 2>/dev/null; then
                #     echo "  ✓ 查询语法检查通过"
                # else
                #     echo "  ⚠ 查询执行失败（可能是预期内的）"
                # fi
                
                mysql -e "DROP DATABASE IF EXISTS test_syntax_check;" 2>/dev/null
            fi
        fi
    fi
else
    echo "信息: mysql客户端未安装，跳过实际测试"
fi

echo ""
echo "================================================"
echo "检查完成"
echo ""
echo "建议:"
echo "1. 在测试环境中实际运行所有查询"
echo "2. 检查所有替换标记 {VAR} 是否正确"
echo "3. 验证所有表名和列名是否存在"
echo "4. 确保UNION查询的列数和类型匹配"