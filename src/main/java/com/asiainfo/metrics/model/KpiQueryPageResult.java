package com.asiainfo.metrics.model;

import io.quarkus.runtime.annotations.RegisterForReflection;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 *
 *
 * @author QvQ
 * @date 2025/11/11
 */
@RegisterForReflection
public record KpiQueryPageResult(List<Map<String, Object>> dataArray, // 数据数组
                                 boolean hasNext, // 是否有下一页
                                 long total, // 总记录数
                                 Set<String> schema // 字段集合（仅限横表模式）
) {

    /**
     * 创建成功结果
     */
    public static KpiQueryPageResult success(List<Map<String, Object>> dataArray, boolean hasNext, long total) {
        return new KpiQueryPageResult(dataArray, hasNext, total, null);
    }

    /**
     * 创建横表成功结果
     */
    public static KpiQueryPageResult successWideTable(List<Map<String, Object>> dataArray, boolean hasNext, long total, Set<String> schema) {
        return new KpiQueryPageResult(dataArray, hasNext, total, schema);
    }

    /**
     * 创建空结果
     */
    public static KpiQueryPageResult empty() {
        return new KpiQueryPageResult(List.of(), false, 0, null);
    }
}