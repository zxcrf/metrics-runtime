package com.asiainfo.metrics.api.dto;

import io.quarkus.runtime.annotations.RegisterForReflection;

import java.util.List;
import java.util.Map;

/**
 * KPI查询结果
 */
@RegisterForReflection
public record KpiQueryResult(
    List<Map<String, Object>> dataArray, // 数据数组
    String status, // 业务状态码
    String msg // 如 查询成功！耗时 xx ms
) {

    /**
     * 创建成功结果
     */
    public static KpiQueryResult success(List<Map<String, Object>> dataArray, String msg) {
        return new KpiQueryResult(dataArray, "0000", msg);
    }

    /**
     * 创建空结果
     */
    public static KpiQueryResult error(String errorMsg) {
        return new KpiQueryResult(List.of(), "9999", errorMsg);
    }

    public static KpiQueryResult empty() {return new  KpiQueryResult(List.of(), "0000", "");}
}
