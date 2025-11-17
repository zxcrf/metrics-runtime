package com.asiainfo.metrics.service;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * KPI计算引擎
 * 使用SQLite内存计算进行嵌套指标计算
 *
 * 核心特性:
 * - 分层计算: 通过临时表支持嵌套依赖
 * - 虚拟线程: 每个请求一个虚拟线程
 * - 内存优化: 使用内存SQLite数据库
 * - 水平扩展: 每个JVM实例独立处理
 */
@ApplicationScoped
public class KpiSQLiteEngine extends AbstractKpiQueryEngineImpl {

    private static final Logger log = LoggerFactory.getLogger(KpiSQLiteEngine.class);
    private static final String NOT_EXISTS = "--";

    @Inject
    SQLiteFileManager sqliteFileManager;

    @Override
    protected String getKpiDataTableName(String kpiId, String cycleType, String compDimCode, String opTime) {
        return sqliteFileManager.getSQLiteTableName(kpiId, opTime, compDimCode);
    }

    @Override
    protected String getDimDataTableName(String compDimCode) {
        // 维度表命名规则：dim_{compDimCode}
        return String.format("kpi_dim_%s", compDimCode);
    }
}
