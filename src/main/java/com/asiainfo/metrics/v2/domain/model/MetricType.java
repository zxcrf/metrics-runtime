package com.asiainfo.metrics.v2.domain.model;

/**
 * 指标类型枚举
 * PHYSICAL: 物理指标 - 存储在数据库中的真实数据
 * VIRTUAL: 虚拟指标 - 纯计算指标，如 ${KD1002} + ${KD1005}
 * COMPOSITE: 复合指标 - 数据库中定义的复合指标，如 KD3000 (KD1002 + KD1005)
 */
public enum MetricType {
    PHYSICAL,   // 物理表存储
    VIRTUAL,    // 纯计算
    COMPOSITE,  // 复合（表达式运算）
    CUMULATIVE  // 月累计指标
}
