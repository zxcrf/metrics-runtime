package com.asiainfo.metrics.service;

import com.asiainfo.metrics.model.http.KpiQueryRequest;

import java.util.*;

/**
 * 统一的结果聚合器，抽离RDB/SQLite引擎的重复聚合逻辑
 */
public class KpiResultAggregator {

    private static final String NOT_EXISTS = "--";

    /**
     * 将扁平化的查询结果按维度聚合
     *
     * @param flatResults 扁平化结果列表：每个条目包含维度字段、op_time、kpi_id、current、lastYear、lastCycle
     * @param request     查询请求（包含维度列表）
     * @return 聚合后的结果列表
     */
    public static List<Map<String, Object>> aggregateResultsByDimensions(
            List<Map<String, Object>> flatResults,
            KpiQueryRequest request) {

        // 获取维度字段列表
        List<String> dimFields = Optional.ofNullable(request.dimCodeArray())
                .filter(dims -> !dims.isEmpty())
                .orElseGet(Collections::emptyList);

        // 按维度+时间分组
        Map<String, Map<String, Object>> aggregatedMap = new LinkedHashMap<>();

        for (Map<String, Object> row : flatResults) {
            // 构建分组键：维度字段 + op_time
            StringBuilder groupKeyBuilder = new StringBuilder();
            for (String dimField : dimFields) {
                Object dimValue = row.get(dimField);
                groupKeyBuilder.append(dimField).append("=").append(dimValue).append("|");
            }

            String opTime = (String) row.get("op_time");
            groupKeyBuilder.append("opTime=").append(opTime).append("|");
            String groupKey = groupKeyBuilder.toString();

            // 获取或创建聚合行
            Map<String, Object> aggregatedRow = aggregatedMap.computeIfAbsent(groupKey, key -> {
                Map<String, Object> newRow = new LinkedHashMap<>();

                // 复制维度字段及描述
                for (String dimField : dimFields) {
                    Object dimValue = row.get(dimField);
                    newRow.put(dimField, dimValue);

                    // 添加维度描述字段（如果存在）
                    String descField = dimField + "_desc";
                    if (row.containsKey(descField)) {
                        newRow.put(descField, row.get(descField));
                    }
                }

                newRow.put("opTime", opTime);
                newRow.put("kpiValues", new LinkedHashMap<String, Map<String, Object>>());
                return newRow;
            });

            // 构建KPI值对象
            String kpiId = (String) row.get("kpi_id");
            Object current = row.get("current");
            Object lastYear = row.get("lastYear");
            Object lastCycle = row.get("lastCycle");
            Object targetValue = row.get("target_value");
            Object checkResult = row.get("check_result");
            Object checkDesc = row.get("check_desc");

            Map<String, Object> kpiValueMap = new LinkedHashMap<>();
            kpiValueMap.put("current", current != null ? current : NOT_EXISTS);
            kpiValueMap.put("lastYear", lastYear != null ? lastYear : NOT_EXISTS);
            kpiValueMap.put("lastCycle", lastCycle != null ? lastCycle : NOT_EXISTS);

            // 如果有目标值相关数据，也添加
            if (targetValue != null || checkResult != null || checkDesc != null) {
                kpiValueMap.put("targetValue", targetValue != null ? targetValue : NOT_EXISTS);
                kpiValueMap.put("checkResult", checkResult != null ? checkResult : NOT_EXISTS);
                kpiValueMap.put("checkDesc", checkDesc != null ? checkDesc : NOT_EXISTS);
            }

            // 添加到kpiValues
            @SuppressWarnings("unchecked")
            Map<String, Map<String, Object>> kpiValues =
                (Map<String, Map<String, Object>>) aggregatedRow.get("kpiValues");
            kpiValues.put(kpiId, kpiValueMap);
        }

        return new ArrayList<>(aggregatedMap.values());
    }
}
