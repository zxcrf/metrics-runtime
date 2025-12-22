package com.asiainfo.metrics.infrastructure.cache.pubsub;

import java.time.Instant;
import java.util.List;

/**
 * 缓存失效事件
 * 用于在集群节点间传播缓存失效消息
 * 
 * 使用 kpiIds + opTimes 组合进行精确失效，避免缓存雪崩
 */
public record CacheInvalidationEvent(
        String kpiModelId,
        List<String> kpiIds,
        List<String> opTimes, // 支持多个时间点
        Instant timestamp) {
    public static CacheInvalidationEvent create(String kpiModelId, List<String> kpiIds, List<String> opTimes) {
        return new CacheInvalidationEvent(kpiModelId, kpiIds, opTimes, Instant.now());
    }
}
