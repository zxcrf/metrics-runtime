package com.asiainfo.metrics.model;

import java.util.List;
import java.util.Map;

/**
 * KPI查询请求
 */
public record KpiQueryRequest(
    List<String> kpiArray, // KPI ID列表
    List<String> opTimeArray, // 时间点列表
    List<String> dimCodeArray, // 维度字段列表，是SQL的聚合字段 如 ["city_id", "county_id"]
    List<DimCondition> dimConditionArray, // 维度条件，多个条件之间是and关系
    Map<String, String> sortOptions // SQL排序选项, 报表才会用到，指标暂时不用
) {

    /**
     * 维度条件
     */
    public record DimCondition(
        String dimConditionCode, //如 "city_id"
        String dimConditionVal   // 如 "4,10"，代表要查询这两个city_id维度的指标值
    ) {}
}
