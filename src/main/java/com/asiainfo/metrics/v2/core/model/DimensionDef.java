package com.asiainfo.metrics.v2.core.model;

/**
 * 维度定义
 */
public record DimensionDef(
    String dimCode,       // 维度代码，如 city_id
    String dimName,       // 维度名称
    String dbColName,     // 数据库列名
    String dimTableName   // 维度表名
) {
    /**
     * 生成维度表名
     * 格式：kpi_dim_{compDimCode}
     */
    public String toDimTableName(String compDimCode) {
        return String.format("kpi_dim_%s", compDimCode);
    }

    /**
     * 生成维度值字段别名
     */
    public String toDimValAlias() {
        return dbColName + "_desc";
    }
}
