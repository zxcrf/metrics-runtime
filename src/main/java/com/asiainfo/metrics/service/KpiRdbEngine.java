package com.asiainfo.metrics.service;

import jakarta.enterprise.context.ApplicationScoped;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * RDB引擎
 * 在数据向MinIO割接过程中，无法使用SQLite存算引擎时，使用元数据库承载存算需求
 * 所有计算在数据库层面完成，直接拼接kpiExpr到SQL中
 *
 * @author QvQ
 * @date 2025/11/11
 */
@ApplicationScoped
public class KpiRdbEngine extends AbstractKpiQueryEngineImpl {

    private static final Logger log = LoggerFactory.getLogger(KpiRdbEngine.class);

    @Override
    protected String getDimDataTableName(String compDimCode) {
        // RDB维度表命名规则：kpi_dim_{compDimCode}
        return String.format("kpi_dim_%s", compDimCode);
    }



}
