package com.asiainfo.metrics.v2.domain.model;

/**
 * 指标定义 (Refactored)
 * 增加 compDimCode 字段，不再依赖外部传递
 */
public record MetricDefinition(
        String id,
        String expression,
        MetricType type,
        String aggFunc,
        String compDimCode // 新增：组合维度编码
) {
    /**
     * 物理指标工厂
     */
    public static MetricDefinition physical(String id, String aggFunc, String compDimCode) {
        return new MetricDefinition(id, "${" + id + ".current}", MetricType.PHYSICAL, aggFunc, compDimCode);
    }

    /**
     * 复合指标工厂
     */
    public static MetricDefinition composite(String id, String expression, String aggFunc, String compDimCode) {
        return new MetricDefinition(id, expression, MetricType.COMPOSITE, aggFunc, compDimCode);
    }

    /**
     * 虚拟指标工厂
     * 虚拟指标通常没有固定的 compDimCode，可以为 null，由其依赖的物理指标决定数据源
     */
    public static MetricDefinition virtual(String id, String expression, String aggFunc) {
        return new MetricDefinition(id, expression, MetricType.VIRTUAL, aggFunc, null);
    }

    // 为了兼容旧代码的辅助构造器（可选，建议逐步迁移）
    public MetricDefinition(String id, String expression, MetricType type, String aggFunc) {
        this(id, expression, type, aggFunc, null);
    }
}