-- 准备集成测试数据（使用正确的字段）

-- 1. 插入测试模型
INSERT IGNORE INTO metrics_model_def (model_id, model_name, model_type, create_time, update_time)
VALUES ('integration_test_table', '集成测试模型', 'PHYSICAL', NOW(), NOW());

-- 2. 插入测试指标（使用正确的字段）
INSERT IGNORE INTO metrics_def (kpi_id, kpi_name, comp_dim_code, model_id, kpi_expr, kpi_type, create_time, update_time)
VALUES 
('KD_TEST_001', '测试指标001', 'CD003', 'integration_test_table', 'value1', 'DERIVED', NOW(), NOW()),
('KD_TEST_002', '测试指标002', 'CD003', 'integration_test_table', 'value2', 'DERIVED', NOW(), NOW());

-- 3. 创建测试源表
CREATE TABLE IF NOT EXISTS integration_test_source (
    id INT AUTO_INCREMENT PRIMARY KEY,
    op_time VARCHAR(8) NOT NULL,
    city_id VARCHAR(50),
    value1 DECIMAL(15,2),
    value2 DECIMAL(15,2),
    create_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_op_time (op_time)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 4. 插入测试源数据
INSERT INTO integration_test_source (op_time, city_id, value1, value2)
VALUES
('20251201', 'city_001', 100.00, 200.00),
('20251201', 'city_002', 150.00, 250.00)
ON DUPLICATE KEY UPDATE value1=VALUES(value1);

-- 5. 查看结果
SELECT '=== 测试数据已准备 ===' AS msg;
SELECT * FROM metrics_model_def WHERE model_id = 'integration_test_table';
SELECT kpi_id, kpi_name, model_id FROM metrics_def WHERE kpi_id LIKE 'KD_TEST_%';
SELECT COUNT(*) AS total FROM integration_test_source;
