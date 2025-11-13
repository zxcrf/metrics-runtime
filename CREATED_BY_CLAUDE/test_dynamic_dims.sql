-- 测试动态维度获取功能
-- 验证从数据库获取维度定义

-- 1. 测试查询组合维度定义
SELECT '=== 测试组合维度定义 ===' as test_name;
SELECT comp_dim_code, comp_dim_name, comp_dim_conf
FROM metrics_comp_dim_def
WHERE comp_dim_code IN ('CD001', 'CD002', 'CD003');

-- 2. 测试查询原子维度定义
SELECT '=== 测试原子维度定义 ===' as test_name;
SELECT dim_code, dim_name, db_col_name, dim_desc
FROM metrics_dim_def
WHERE dim_code IN ('D1001', 'D1002', 'D1003');

-- 3. 测试CD001对应的维度字段
SELECT '=== 测试CD001维度字段 ===' as test_name;
SELECT GROUP_CONCAT(db_col_name ORDER BY dim_code) as dim_fields
FROM metrics_dim_def
WHERE dim_code IN (
    SELECT JSON_UNQUOTE(JSON_EXTRACT(comp_dim_conf, '$[*].dimCode'))
    FROM metrics_comp_dim_def
    WHERE comp_dim_code = 'CD001'
);

-- 4. 测试CD002对应的维度字段
SELECT '=== 测试CD002维度字段 ===' as test_name;
SELECT GROUP_CONCAT(db_col_name ORDER BY dim_code) as dim_fields
FROM metrics_dim_def
WHERE dim_code IN (
    SELECT JSON_UNQUOTE(JSON_EXTRACT(comp_dim_conf, '$[*].dimCode'))
    FROM metrics_comp_dim_def
    WHERE comp_dim_code = 'CD002'
);

-- 5. 测试CD003对应的维度字段
SELECT '=== 测试CD003维度字段 ===' as test_name;
SELECT GROUP_CONCAT(db_col_name ORDER BY dim_code) as dim_fields
FROM metrics_dim_def
WHERE dim_code IN (
    SELECT JSON_UNQUOTE(JSON_EXTRACT(comp_dim_conf, '$[*].dimCode'))
    FROM metrics_comp_dim_def
    WHERE comp_dim_code = 'CD003'
);

SELECT '=== 测试完成 ===' as test_name;
