-- 完整的ETL集成测试数据准备（修正版）
-- 符合业务规范：组合维度 = 多个原子维度组合，KPI编码规范

-- ============================================
-- 1. 创建测试宽表（源表）
-- ============================================
DROP TABLE IF EXISTS test_sales_wide_table;
CREATE TABLE test_sales_wide_table (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    op_time VARCHAR(8) NOT NULL COMMENT '数据日期',
    region_id VARCHAR(20) COMMENT '省份ID',
    city_id VARCHAR(20) COMMENT '城市ID',
    county_id VARCHAR(20) COMMENT '县域ID',
    product_id VARCHAR(20) COMMENT '产品ID',
    sales_amount DECIMAL(15,2) COMMENT '销售额',
    sales_count INT COMMENT '销售数量',
    customer_count INT COMMENT '客户数',
    order_count INT COMMENT '订单数',
    create_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_op_time (op_time),
    INDEX idx_region (region_id),
    INDEX idx_city (city_id),
    INDEX idx_county (county_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='测试销售宽表';

-- 插入测试数据（省份-城市-县域三级维度）
INSERT INTO test_sales_wide_table (op_time, region_id, city_id, county_id, product_id, sales_amount, sales_count, customer_count, order_count)
VALUES
-- 20251201数据
('20251201', 'R01', 'C001', 'T0001', 'P001', 150000.00, 1500, 300, 450),
('20251201', 'R01', 'C001', 'T0002', 'P001', 120000.00, 1200, 250, 380),
('20251201', 'R01', 'C002', 'T0003', 'P002', 180000.00, 900, 200, 320),
('20251201', 'R02', 'C003', 'T0004', 'P001', 200000.00, 2000, 400, 600),
('20251201', 'R02', 'C003', 'T0005', 'P002', 90000.00, 600, 150, 220),
-- 20251202数据  
('20251202', 'R01', 'C001', 'T0001', 'P001', 160000.00, 1600, 320, 480),
('20251202', 'R01', 'C001', 'T0002', 'P001', 125000.00, 1250, 260, 390),
('20251202', 'R01', 'C002', 'T0003', 'P002', 190000.00, 950, 210, 340),
('20251202', 'R02', 'C003', 'T0004', 'P001', 210000.00, 2100, 420, 630),
('20251202', 'R02', 'C003', 'T0005', 'P002', 95000.00, 630, 160, 230),
-- 20251203数据
('20251203', 'R01', 'C001', 'T0001', 'P001', 155000.00, 1550, 310, 465),
('20251203', 'R01', 'C001', 'T0002', 'P001', 122000.00, 1220, 255, 385),
('20251203', 'R01', 'C002', 'T0003', 'P002', 185000.00, 920, 205, 330),
('20251203', 'R02', 'C003', 'T0004', 'P001', 205000.00, 2050, 410, 615),
('20251203', 'R02', 'C003', 'T0005', 'P002', 92000.00, 610, 155, 225);

-- ============================================
-- 2. 配置原子维度（按业务规范）
-- ============================================
DELETE FROM metrics_dim_def WHERE dim_code IN ('D0001', 'D1001', 'D1002', 'D1003');

INSERT INTO metrics_dim_def (dim_code, dim_name, dim_type, dim_val_type, db_col_name, t_state, create_time, update_time)
VALUES
('D0001', '时间', 'Temporal', 'CONST', 'op_time', '1', NOW(), NOW()),
('D1001', '城市', 'Spatial', 'CONST', 'city_id', '1', NOW(), NOW()),
('D1002', '县域', 'Spatial', 'CONST', 'county_id', '1', NOW(), NOW()),
('D1003', '省份', 'Spatial', 'CONST', 'region_id', '1', NOW(), NOW());

-- ============================================
-- 3. 配置组合维度（省份+城市+县域）
-- ============================================
DELETE FROM metrics_comp_dim_def WHERE comp_dim_code = 'CD_TEST';

INSERT INTO metrics_comp_dim_def (comp_dim_code, comp_dim_name, comp_dim_conf, create_time, update_time)
VALUES ('CD_TEST', '测试组合维度', '[{"dimCode":"D1003"},{"dimCode":"D1001"},{"dimCode":"D1002"}]', NOW(), NOW());

-- ============================================
-- 4. 配置模型定义
-- ============================================
DELETE FROM metrics_model_def WHERE model_id = 'test_sales_wide_table';

INSERT INTO metrics_model_def (model_id, model_name, model_type, comp_dim_code, model_ds_name, model_sql, t_state, create_time, update_time)
VALUES ('test_sales_wide_table', '测试销售宽表模型', 'PHYSICAL', 'CD_TEST', 'DATADB', 
'SELECT op_time, region_id, city_id, county_id, SUM(sales_amount) as sales_amount, SUM(sales_count) as sales_count, SUM(customer_count) as customer_count, SUM(order_count) as order_count FROM test_sales_wide_table WHERE op_time = ${op_time} GROUP BY op_time, ${dimGroup}', 
'1', NOW(), NOW());

-- ============================================
-- 5. 配置派生指标（规范命名：KD+四位数字）
-- ============================================
DELETE FROM metrics_def WHERE kpi_id IN ('KD0001', 'KD0002', 'KD0003', 'KD0004');

INSERT INTO metrics_def (kpi_id, kpi_name, comp_dim_code, model_id, kpi_expr, kpi_unit, cycle_type, compute_method, kpi_type, t_state, create_time, update_time, agg_func)
VALUES
('KD0001', '日销售额', 'CD_TEST', 'test_sales_wide_table', 'sales_amount', '元', 'DAY', 'SUM', 'EXTENDED', '1', NOW(), NOW(), 'sum'),
('KD0002', '日销售数量', 'CD_TEST', 'test_sales_wide_table', 'sales_count', '件', 'DAY', 'SUM', 'EXTENDED', '1', NOW(), NOW(), 'sum'),
('KD0003', '日客户数', 'CD_TEST', 'test_sales_wide_table', 'customer_count', '人', 'DAY', 'SUM', 'EXTENDED', '1', NOW(), NOW(), 'sum'),
('KD0004', '日订单数', 'CD_TEST', 'test_sales_wide_table', 'order_count', '单', 'DAY', 'SUM', 'EXTENDED', '1', NOW(), NOW(), 'sum');

-- ============================================
-- 6. 配置维度翻译数据（city_id的属性）
-- ============================================
CREATE TABLE IF NOT EXISTS kpi_dim_CD_TEST (
    dim_code VARCHAR(32) COMMENT '维度编码',
    dim_val VARCHAR(128) COMMENT '维度值/翻译',
    dim_id VARCHAR(32) COMMENT '维度ID',
    parent_dim_code VARCHAR(32) COMMENT '父维度编码',
    PRIMARY KEY (dim_code, dim_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

DELETE FROM kpi_dim_CD_TEST;
INSERT INTO kpi_dim_CD_TEST (dim_code, dim_val, dim_id, parent_dim_code) VALUES
-- 省份
('R01', '浙江省', 'region_id', NULL),
('R02', '江苏省', 'region_id', NULL),
-- 城市
('C001', '杭州市', 'city_id', 'R01'),
('C002', '宁波市', 'city_id', 'R01'),
('C003', '南京市', 'city_id', 'R02'),
-- 县域
('T0001', '西湖区', 'county_id', 'C001'),
('T0002', '滨江区', 'county_id', 'C001'),
('T0003', '海曙区', 'county_id', 'C002'),
('T0004', '玄武区', 'county_id', 'C003'),
('T0005', '秦淮区', 'county_id', 'C003');

-- ============================================
-- 7. 验证数据
-- ============================================
SELECT '=== 宽表数据验证 ===' AS msg;
SELECT op_time, COUNT(*) as row_count, SUM(sales_amount) as total_sales 
FROM test_sales_wide_table GROUP BY op_time ORDER BY op_time;

SELECT '=== 组合维度配置验证（省份+城市+县域）===' AS msg;
SELECT comp_dim_code, comp_dim_name, comp_dim_conf FROM metrics_comp_dim_def WHERE comp_dim_code='CD_TEST';

SELECT '=== 原子维度配置验证 ===' AS msg;
SELECT dim_code, dim_name, db_col_name FROM metrics_dim_def WHERE dim_code IN ('D0001', 'D1001', 'D1002', 'D1003');

SELECT '=== 模型配置验证 ===' AS msg;
SELECT model_id, model_name, comp_dim_code, t_state FROM metrics_model_def WHERE model_id = 'test_sales_wide_table';

SELECT '=== 指标配置验证（规范编码）===' AS msg;
SELECT kpi_id, kpi_name, model_id, kpi_type, agg_func FROM metrics_def WHERE model_id = 'test_sales_wide_table';

SELECT '=== 维度翻译数据验证 ===' AS msg;
SELECT dim_id, COUNT(*) as count FROM kpi_dim_CD_TEST GROUP BY dim_id;
