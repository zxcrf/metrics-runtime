package com.asiainfo.metrics.v2.core.model;

/**
 * 指标定义
 * 用于描述一个KPI指标的元数据
 */
public record MetricDefinition(
    String id,           // 指标ID，如 KD1002
    String expression,   // 表达式，如 ${KD1002} + ${KD1005} 或 KD1002
    MetricType type,     // 指标类型
    String aggFunc       // 聚合函数，如 sum, max, min 等
) {
    /**
     * 快捷工厂：创建物理指标定义
     * 物理指标的表达式格式为 ${kpi_id.current}
     */
    public static MetricDefinition physical(String id, String aggFunc) {
        return new MetricDefinition(id, "${" + id + ".current}", MetricType.PHYSICAL, aggFunc);
    }

    /**
     * 快捷工厂：创建复合指标定义
     */
    public static MetricDefinition composite(String id, String expression, String aggFunc) {
        return new MetricDefinition(id, expression, MetricType.COMPOSITE, aggFunc);
    }

    /**
     * 快捷工厂：创建虚拟指标定义
     */
    public static MetricDefinition virtual(String id, String expression, String aggFunc) {
        return new MetricDefinition(id, expression, MetricType.VIRTUAL, aggFunc);
    }
}
