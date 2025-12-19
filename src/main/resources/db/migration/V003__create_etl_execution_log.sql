-- ETL执行日志表
-- 用于记录每次ETL执行的详细信息，支持重做检测和审计追踪

CREATE TABLE IF NOT EXISTS kpi_etl_execution_log (
    id BIGINT AUTO_INCREMENT PRIMARY KEY COMMENT '主键ID',
    kpi_model_id VARCHAR(50) NOT NULL COMMENT '指标模型ID',
    op_time VARCHAR(8) NOT NULL COMMENT '批次时间 YYYYMMDD',
    execution_type ENUM('INITIAL', 'REDO') NOT NULL COMMENT '执行类型：首次/重做',
    status ENUM('RUNNING', 'SUCCESS', 'FAILED') NOT NULL COMMENT '执行状态',
    record_count INT DEFAULT 0 COMMENT '处理记录数',
    error_message TEXT COMMENT '错误信息',
    execution_time_ms BIGINT COMMENT '执行耗时(ms)',
    started_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '开始时间',
    completed_at TIMESTAMP NULL COMMENT '完成时间',
    operator VARCHAR(50) DEFAULT 'system' COMMENT '操作人',
    remark TEXT COMMENT '备注',
    
    INDEX idx_model_optime (kpi_model_id, op_time),
    INDEX idx_status (status),
    INDEX idx_started_at (started_at),
    INDEX idx_execution_type (execution_type)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='指标ETL执行日志';
