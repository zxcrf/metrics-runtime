-- srcTableComplete API 重构相关表
-- 根据 FEAT_SRC_TABLE_COMPLETE.md 技术文档创建

-- 1. metrics_etl_log - ETL表到达日志
-- 使用 UPSERT 语义：首次 INSERT，再次 UPDATE arrival_time
CREATE TABLE IF NOT EXISTS metrics_etl_log (
    id INT AUTO_INCREMENT PRIMARY KEY COMMENT '主键',
    table_name VARCHAR(255) NOT NULL COMMENT '来源表名',
    op_time VARCHAR(255) NOT NULL COMMENT '批次号',
    arrival_time DATETIME NOT NULL COMMENT '表到达时间（ETL重跑时更新）',
    UNIQUE KEY uk_table_optime (table_name, op_time),
    INDEX idx_table_name (table_name),
    INDEX idx_op_time (op_time)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='ETL表到达日志';

-- 2. metrics_model_dependency - 模型依赖表
-- 记录每个模型依赖哪些来源表
CREATE TABLE IF NOT EXISTS metrics_model_dependency (
    id INT AUTO_INCREMENT PRIMARY KEY COMMENT '主键',
    model_id VARCHAR(255) NOT NULL COMMENT '指标模型ID',
    dependency_table_name VARCHAR(255) NOT NULL COMMENT '依赖的来源表名',
    INDEX idx_dependency_table_name (dependency_table_name),
    INDEX idx_model_id (model_id),
    UNIQUE KEY uk_model_table (model_id, dependency_table_name)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='模型依赖表';

-- 3. metrics_task_log - 任务执行日志
-- 记录每次模型执行的状态，支持并发控制和重跑检测
CREATE TABLE IF NOT EXISTS metrics_task_log (
    id INT AUTO_INCREMENT PRIMARY KEY COMMENT '主键',
    model_id VARCHAR(255) NOT NULL COMMENT '模型ID',
    op_time VARCHAR(255) NOT NULL COMMENT '批次号',
    status VARCHAR(255) NOT NULL COMMENT 'RUNNING/SUCCESS/FAILED',
    start_time DATETIME COMMENT '开始时间',
    end_time DATETIME COMMENT '结束时间',
    message VARCHAR(2000) COMMENT '执行消息',
    compute_count INT COMMENT '计算条数',
    storage_count INT COMMENT '存储条数',
    INDEX idx_model_optime (model_id, op_time),
    INDEX idx_status (status),
    INDEX idx_start_time (start_time)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='任务执行日志';

-- 4. metrics_webhook_subscription - Webhook订阅表
-- 支持指标更新通知
CREATE TABLE IF NOT EXISTS metrics_webhook_subscription (
    id INT AUTO_INCREMENT PRIMARY KEY COMMENT '主键',
    kpi_id VARCHAR(64) NOT NULL COMMENT '订阅的指标ID',
    callback_url VARCHAR(512) NOT NULL COMMENT '回调地址',
    secret VARCHAR(128) COMMENT '鉴权三元组，格式: 类型,键,值',
    retry_num INT DEFAULT 3 COMMENT '重试次数',
    status VARCHAR(16) DEFAULT '1' COMMENT '1=启用，0=禁用',
    team_name VARCHAR(64) COMMENT '租户标识',
    user_id VARCHAR(64) COMMENT '用户ID',
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    INDEX idx_kpi_id (kpi_id),
    INDEX idx_status (status)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='Webhook订阅表';
