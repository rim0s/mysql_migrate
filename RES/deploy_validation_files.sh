#!/bin/bash
# 验证文件部署脚本
# 位置: RES/deploy_validation_files.sh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# 文件定义
VALIDATION_LIB="$SCRIPT_DIR/mysql_validation_functions.sh"
VALIDATION_QUERIES="$SCRIPT_DIR/mysql_validation_queries.sql"
HASH_FILE="$SCRIPT_DIR/mysql_validation_queries.sha256"

# 计算哈希
calculate_hash() {
    if [[ ! -f "$VALIDATION_QUERIES" ]]; then
        echo "错误: 查询文件不存在: $VALIDATION_QUERIES"
        return 1
    fi
    
    sha256sum "$VALIDATION_QUERIES" | cut -d' ' -f1
}

# 验证文件完整性
verify_integrity() {
    echo "验证文件完整性..."
    echo ""
    
    # 检查必需文件
    local missing_files=()
    [[ ! -f "$VALIDATION_LIB" ]] && missing_files+=("mysql_validation_functions.sh")
    [[ ! -f "$VALIDATION_QUERIES" ]] && missing_files+=("mysql_validation_queries.sql")
    
    if [[ ${#missing_files[@]} -gt 0 ]]; then
        echo "错误: 以下文件缺失："
        for file in "${missing_files[@]}"; do
            echo "  - $file"
        done
        return 1
    fi
    
    echo "✓ 所有必需文件都存在"
    
    # 计算当前哈希
    local current_hash=$(calculate_hash)
    echo "当前文件哈希: $current_hash"
    
    # 检查哈希文件
    if [[ -f "$HASH_FILE" ]]; then
        local stored_hash=$(cat "$HASH_FILE")
        if [[ "$current_hash" == "$stored_hash" ]]; then
            echo "✓ 哈希验证通过"
        else
            echo "⚠ 哈希不匹配！"
            echo "存储的哈希: $stored_hash"
            echo "当前的哈希: $current_hash"
            echo ""
            echo "文件可能已被修改，请确认是否继续"
            read -p "是否更新哈希文件? (yes/no): " update_hash
            if [[ "$update_hash" =~ ^(yes|YES|y|Y)$ ]]; then
                echo "$current_hash" > "$HASH_FILE"
                echo "哈希文件已更新"
            fi
        fi
    else
        echo "$current_hash" > "$HASH_FILE"
        echo "✓ 哈希文件已创建"
    fi
    
    echo ""
    echo "部署完成！"
    echo "文件位置: $SCRIPT_DIR"
    ls -lh "$SCRIPT_DIR/" | grep -E "(mysql_validation|\.sha256)" | sed 's/^/  /'
}

# 主函数
main() {
    echo "================================================"
    echo "    MySQL 验证文件部署工具"
    echo "================================================"
    echo "脚本目录: $SCRIPT_DIR"
    echo ""
    
    verify_integrity
}

main "$@"