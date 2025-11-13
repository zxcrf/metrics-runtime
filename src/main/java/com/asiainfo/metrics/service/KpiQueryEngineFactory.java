package com.asiainfo.metrics.service;

import com.asiainfo.metrics.config.MetricsConfig;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * KPI查询引擎工厂
 * 根据配置动态选择合适的查询引擎（MySQL或SQLite）
 *
 * @author QvQ
 * @date 2025/11/12
 */
@ApplicationScoped
public class KpiQueryEngineFactory {

    private static final Logger log = LoggerFactory.getLogger(KpiQueryEngineFactory.class);

    @Inject
    MetricsConfig metricsConfig;

    @Inject
    KpiRdbEngine kpiRdbEngine;

    @Inject
    KpiSQLiteEngine kpiSQLiteEngine;

    /**
     * 获取当前配置的查询引擎
     *
     * @return KpiQueryEngine 实例
     */
    public KpiQueryEngine getQueryEngine() {
        String engineType = metricsConfig.getCurrentEngine();
        log.info("当前存算引擎类型: {}", engineType);

        KpiQueryEngine engine;
        if (metricsConfig.isSQLiteEnabled()) {
            log.info("使用SQLite查询引擎");
            engine = kpiSQLiteEngine;
        } else {
            log.info("使用MySQL/RDB查询引擎");
            engine = kpiRdbEngine;
        }

        return engine;
    }

    /**
     * 获取引擎类型描述
     */
    public String getEngineDescription() {
        if (metricsConfig.isSQLiteEnabled()) {
            return "SQLite引擎 (内存计算)";
        } else {
            return "MySQL/RDB引擎 (关系型数据库)";
        }
    }
}
