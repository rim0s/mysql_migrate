-- MySQL 迁移验证查询脚本
-- 文件名: mysql_validation_queries.sql
-- 注意：此文件只包含 SELECT 查询，不包含任何修改操作

-- ============================================
-- 1. 基本信息查询
-- ============================================

-- 查询 1.1: 系统基本信息（优化版，更好的列名）
SELECT 
    'MySQL 版本' AS '检查项目',
    CONCAT(@@version, ' (', @@version_comment, ')') AS '详细信息'
UNION
SELECT '数据目录', @@datadir
UNION
SELECT 'Socket文件', @@socket
UNION
SELECT '服务端口', @@port
UNION
SELECT '字符集', CONCAT(@@character_set_server, ' (', @@collation_server, ')')
UNION
SELECT '存储引擎', @@default_storage_engine
UNION
SELECT '日志模式', IF(@@log_bin = 1, '已启用', '未启用');

-- ============================================
-- 2. 数据库统计查询
-- ============================================

-- 查询 2.1: 数据库分类统计
SELECT 
    '数据库类型' AS '分类',
    '数量' AS '计数',
    '说明' AS '备注'
UNION ALL
SELECT 
    '业务数据库',
    COUNT(*),
    '用户创建的数据库'
FROM information_schema.SCHEMATA 
WHERE SCHEMA_NAME NOT IN ('information_schema', 'performance_schema', 'sys', 'mysql')
UNION ALL
SELECT 
    '系统数据库',
    COUNT(*),
    'MySQL系统数据库'
FROM information_schema.SCHEMATA 
WHERE SCHEMA_NAME IN ('information_schema', 'performance_schema', 'sys', 'mysql');

-- 查询 2.2: 详细数据库列表
SELECT 
    SCHEMA_NAME AS '数据库名',
    DEFAULT_CHARACTER_SET_NAME AS '字符集',
    DEFAULT_COLLATION_NAME AS '排序规则',
    ROUND(SUM(data_length + index_length) / 1024 / 1024, 2) AS '大小(MB)',
    COUNT(TABLE_NAME) AS '表数量'
FROM information_schema.SCHEMATA s
LEFT JOIN information_schema.TABLES t ON s.SCHEMA_NAME = t.TABLE_SCHEMA
WHERE s.SCHEMA_NAME NOT IN ('information_schema', 'performance_schema', 'sys')
GROUP BY s.SCHEMA_NAME, s.DEFAULT_CHARACTER_SET_NAME, s.DEFAULT_COLLATION_NAME
ORDER BY 4 DESC  -- 使用列位置而不是别名
LIMIT 100;

-- ============================================
-- 3. 用户和权限查询
-- ============================================

-- 查询 3.1: 用户列表
SELECT 
    User AS '用户名',
    Host AS '允许主机',
    plugin AS '认证插件',
    CONVERT(IF(account_locked='Y', '已锁定', '正常') USING utf8mb4) AS '账户状态',
    CONVERT(IF(password_expired='Y', '已过期', '有效') USING utf8mb4) AS '密码状态',
    password_last_changed AS '密码最后修改',
    max_connections AS '最大连接数',
    CONCAT(User, '@', Host) AS '完整账户'
FROM mysql.user 
WHERE User NOT LIKE 'mysql.%' 
    AND User != ''
ORDER BY User, Host
LIMIT 50;

-- 查询 3.2: 数据库权限详情（修复版，避免 only_full_group_by 错误）
SELECT 
    GRANTEE AS '授权账户',
    TABLE_SCHEMA AS '数据库名',
    SUBSTRING_INDEX(GROUP_CONCAT(DISTINCT PRIVILEGE_TYPE ORDER BY PRIVILEGE_TYPE SEPARATOR ', '), ',', 10) AS '权限列表',
    MAX(IS_GRANTABLE) AS '授权权限'
FROM information_schema.SCHEMA_PRIVILEGES
WHERE GRANTEE NOT LIKE "'mysql.%'"
    AND GRANTEE NOT LIKE "'%root%'"
GROUP BY GRANTEE, TABLE_SCHEMA
ORDER BY GRANTEE, TABLE_SCHEMA
LIMIT 100;

-- ============================================
-- 4. 存储和性能查询
-- ============================================

-- 查询 4.1: 表空间信息
SELECT 
    FILE_NAME AS '文件名',
    TABLESPACE_NAME AS '表空间名',
    ENGINE AS '存储引擎',
    ROUND(INITIAL_SIZE / 1024 / 1024, 2) AS '初始大小(MB)',
    ROUND(DATA_FREE / 1024 / 1024, 2) AS '空闲空间(MB)'
FROM information_schema.FILES
WHERE FILE_TYPE = 'DATAFILE'
ORDER BY ENGINE, FILE_NAME
LIMIT 100;

-- 查询 4.2: 当前连接统计
SELECT 
    USER AS '用户',
    HOST AS '主机',
    db AS '当前数据库',
    COUNT(*) AS '连接数',
    GROUP_CONCAT(DISTINCT COMMAND SEPARATOR ', ') AS '命令类型'
FROM information_schema.PROCESSLIST
WHERE USER IS NOT NULL 
    AND USER != 'system user'
GROUP BY USER, HOST, db
ORDER BY COUNT(*) DESC
LIMIT 50;

-- ============================================
-- 5. 数据完整性抽样查询
-- ============================================

-- 查询 5.1: 获取一个非系统数据库进行抽样
SELECT SCHEMA_NAME AS '抽样数据库'
FROM information_schema.SCHEMATA 
WHERE SCHEMA_NAME NOT IN ('information_schema', 'performance_schema', 'sys', 'mysql')
ORDER BY SCHEMA_NAME
LIMIT 1;

-- ============================================
-- 6. 复制状态查询（如果配置）
-- ============================================

-- 查询 6.1: 主从复制状态
-- SHOW SLAVE STATUS;

-- 查询 6.2: 二进制日志状态
-- SHOW MASTER STATUS;

-- ============================================
-- 7. 关键配置查询
-- ============================================

-- 查询 7.1: 缓冲池配置
SELECT 
    'innodb_buffer_pool_size' AS '配置项',
    FORMAT_BYTES(@@innodb_buffer_pool_size) AS '当前值',
    'InnoDB缓冲池大小' AS '说明'
UNION
SELECT 
    'max_connections',
    @@max_connections,
    '最大连接数'
UNION
SELECT 
    'table_open_cache',
    @@table_open_cache,
    '表缓存大小';

-- ============================================
-- 8. 迁移验证专用查询
-- ============================================

-- 查询 8.1: 数据目录验证
SELECT 
    '数据目录验证' AS '检查项目',
    CASE 
        WHEN TRIM(TRAILING '/' FROM @@datadir) = TRIM(TRAILING '/' FROM '{EXPECTED_DIR}') THEN '✓ 通过'
        ELSE '✗ 失败'
    END AS '验证结果',
    TRIM(TRAILING '/' FROM '{EXPECTED_DIR}') AS '期望目录',
    TRIM(TRAILING '/' FROM @@datadir) AS '实际目录',
    IF(TRIM(TRAILING '/' FROM @@datadir) = TRIM(TRAILING '/' FROM '{EXPECTED_DIR}'), 
       '路径匹配', 
       CONCAT('路径不匹配，差异: ', 
              CHAR(10), '期望: ', TRIM(TRAILING '/' FROM '{EXPECTED_DIR}'),
              CHAR(10), '实际: ', TRIM(TRAILING '/' FROM @@datadir)
       )
    ) AS '详细说明';

-- 查询 8.2: 服务状态验证
SELECT 
    '服务状态' AS '检查项目',
    IF(@@version IS NOT NULL, '✓ 运行中', '✗ 停止') AS '状态',
    NOW() AS '检查时间';

-- 查询 8.3: 连接验证
SELECT 
    '数据库连接' AS '检查项目',
    IF(COUNT(*) > 0, '✓ 连接正常', '✗ 连接失败') AS '状态',
    COUNT(*) AS '连接数'
FROM information_schema.PROCESSLIST 
WHERE ID = CONNECTION_ID();

-- ============================================
-- 9. 系统状态查询
-- ============================================

-- 查询 9.1: 运行时间
SELECT 
    '服务器运行时间' AS '状态',
    CONCAT(
        FLOOR(VARIABLE_VALUE / 86400), '天 ',
        FLOOR((VARIABLE_VALUE % 86400) / 3600), '小时 ',
        FLOOR((VARIABLE_VALUE % 3600) / 60), '分钟'
    ) AS '值'
FROM performance_schema.global_status 
WHERE VARIABLE_NAME = 'Uptime';

-- 查询 9.2: 查询统计
SELECT 
    '查询统计' AS '类型',
    VARIABLE_NAME AS '查询类型',
    VARIABLE_VALUE AS '执行次数'
FROM performance_schema.global_status 
WHERE VARIABLE_NAME LIKE 'COM_%' 
    AND VARIABLE_VALUE > 0
ORDER BY VARIABLE_VALUE DESC
LIMIT 5;
