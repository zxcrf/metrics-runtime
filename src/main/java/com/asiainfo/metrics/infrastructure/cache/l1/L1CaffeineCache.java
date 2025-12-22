package com.asiainfo.metrics.infrastructure.cache.l1;

import com.asiainfo.metrics.infrastructure.cache.CacheConfig;
import com.asiainfo.metrics.infrastructure.cache.CacheKey;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import jakarta.annotation.PostConstruct;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

/**
 * L1 Caffeine 内存缓存
 * 提供毫秒级响应的热点数据缓存
 */
@ApplicationScoped
public class L1CaffeineCache {

    private static final Logger log = LoggerFactory.getLogger(L1CaffeineCache.class);

    @Inject
    CacheConfig config;

    private Cache<String, Object> cache;

    @PostConstruct
    void init() {
        cache = Caffeine.newBuilder()
                .expireAfterWrite(config.getL1TtlSeconds(), TimeUnit.SECONDS)
                .maximumSize(config.getL1MaxSize())
                .recordStats()
                .build();

        log.info("[L1 Cache] Initialized with TTL={}s, MaxSize={}",
                config.getL1TtlSeconds(), config.getL1MaxSize());
    }

    /**
     * 获取缓存
     */
    @SuppressWarnings("unchecked")
    public <T> T get(CacheKey key, Class<T> type) {
        Object value = cache.getIfPresent(key.toL1Key());
        if (value != null) {
            log.debug("[L1 Cache] Hit: {}", key);
            return (T) value;
        }
        log.debug("[L1 Cache] Miss: {}", key);
        return null;
    }

    /**
     * 写入缓存
     */
    public void put(CacheKey key, Object value) {
        cache.put(key.toL1Key(), value);
        log.debug("[L1 Cache] Put: {}", key);
    }

    /**
     * 失效指定KPI和时间的缓存
     */
    public void invalidate(String kpiId, String opTime) {
        long count = cache.asMap().keySet().stream()
                .filter(k -> k.contains("kpis:") && k.contains(kpiId) &&
                        k.contains("times:") && k.contains(opTime))
                .peek(cache::invalidate)
                .count();

        if (count > 0) {
            log.info("[L1 Cache] Invalidated {} keys for kpi={}, opTime={}", count, kpiId, opTime);
        }
    }

    /**
     * 清空所有缓存
     */
    public void invalidateAll() {
        cache.invalidateAll();
        log.info("[L1 Cache] Invalidated all");
    }

    /**
     * 获取缓存统计
     */
    public String getStats() {
        var stats = cache.stats();
        return String.format("L1 Stats: hitRate=%.2f%%, size=%d, hits=%d, misses=%d",
                stats.hitRate() * 100,
                cache.estimatedSize(),
                stats.hitCount(),
                stats.missCount());
    }
}
