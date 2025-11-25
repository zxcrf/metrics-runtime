package com.asiainfo.metrics.v2.core.model;

/**
 * 物理表请求
 * 表示需要从物理表加载数据的请求
 */
public record PhysicalTableReq(
    String kpiId,        // KPI ID
    String opTime,       // 操作时间，如 20251024
    String compDimCode   // 复合维度代码，如 CD003
) {
    /**
     * 生成表名
     * 格式：kpiId_opTime_compDimCode
     * 例如：KD1002_20251024_CD003
     */
    public String toTableName() {
        return String.format("kpi_%s_%s_%s", kpiId, opTime, compDimCode);
    }
}
