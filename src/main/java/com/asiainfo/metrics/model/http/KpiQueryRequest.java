package com.asiainfo.metrics.model.http;

import io.quarkus.runtime.annotations.RegisterForReflection;

import java.util.List;
import java.util.Map;

/**
 * KPI查询请求
 */
@RegisterForReflection
public record KpiQueryRequest(
    List<String> kpiArray, // KPI ID列表
    List<String> opTimeArray, // 时间点列表
    List<String> dimCodeArray, // 维度字段列表，是SQL的聚合字段 如 ["city_id", "county_id"]
    List<DimCondition> dimConditionArray, // 维度条件，多个条件之间是and关系
    Map<String, String> sortOptions, // SQL排序选项, 报表才会用到，指标暂时不用
    Boolean includeHistoricalData, // 是否包含历史数据（lastCycle和lastYear），默认true
    Boolean includeTargetData // 是否包含目标值相关数据（target_value、check_result、check_desc），默认false
) {

    /**
     * 维度条件
     */
    public record DimCondition(
        String dimConditionCode, //如 "city_id"
        String dimConditionVal   // 如 "4,10"，代表要查询这两个city_id维度的指标值
    ) {}
}
