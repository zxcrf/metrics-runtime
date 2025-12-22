package com.asiainfo.metrics.domain.model;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * KPI指标定义
 * 对应metrics_def表
 */
public record KpiDefinition(
    @JsonProperty String kpiId,
    @JsonProperty String kpiName,
    @JsonProperty String kpiType, // EXTENDED, COMPUTED, EXPRESSION, COMPOSITE
    @JsonProperty String compDimCode, // CD003
    @JsonProperty String cycleType, // DAY, MONTH, YEAR
    @JsonProperty String topicId,
    @JsonProperty String teamName,
    @JsonProperty String kpiExpr, // 指标表达式，派生指标KD1002的表达式如sum(KD1002) / count(distinct case when x=y then 1 else 0 end) / avg(KD1002)
                              // 复合指标KD2002的表达式如 KD1002 * 0.7 / (KD1002 + 100) 需要转换为sum(KD1002) * 0.7 / (sum(KD1002) + 100)
                              // 复合指标KD2003的表达式如 KD2002 / (1+KD2002)，需要转换为 (sum(KD1002) * 0.7 / (sum(KD1002) + 100)) / (1+sum(KD1002) * 0.7 / (sum(KD1002) + 100))
                              // 表达式指标${KD2002.lastCycle}/(${KD2003} + ${KD2002})，就需要获取KD2002的上一个周期的值作为分子， KD2003和KD2002当前批次的值作为分母
    @JsonProperty String computeMethod, // 计算方法：normal, expr, cumulative
    @JsonProperty String aggFunc, // 聚合函数：sum(可加), first_value/last_value(半可加), min/max/last_value/first_value(不可加)
    @JsonProperty String createTime,
    @JsonProperty String updateTime
) {}
