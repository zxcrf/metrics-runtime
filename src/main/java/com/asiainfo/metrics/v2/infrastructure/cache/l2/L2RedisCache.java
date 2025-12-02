package com.asiainfo.metrics.v2.infrastructure.cache.l2;

import com.asiainfo.metrics.v2.infrastructure.cache.CacheConfig;
import com.asiainfo.metrics.v2.infrastructure.cache.CacheKey;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.quarkus.redis.datasource.RedisDataSource;
import io.quarkus.redis.datasource.keys.KeyCommands;
import io.quarkus.redis.datasource.value.ValueCommands;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * L2 Redis 分布式缓存
 * 提供跨节点共享的分布式缓存
 */
@ApplicationScoped
public class L2RedisCache {

    private static final Logger log = LoggerFactory.getLogger(L2RedisCache.class);

    @Inject
    CacheConfig config;

    @Inject
    RedisDataSource redis;

    @Inject
    ObjectMapper objectMapper;

    private final ExecutorService asyncExecutor = Executors.newVirtualThreadPerTaskExecutor();

    /**
     * 获取缓存
     */
    public <T> T get(CacheKey key, TypeReference<T> typeRef) {
        try {
            ValueCommands<String, String> commands = redis.value(String.class);
            String value = commands.get(key.toL2Key());

            if (value != null) {
                log.debug("[L2 Cache] Hit: {}", key);
                return objectMapper.readValue(value, typeRef);
            }
            log.debug("[L2 Cache] Miss: {}", key);
        } catch (Exception e) {
            log.warn("[L2 Cache] Get failed for key {}: {}", key, e.getMessage());
        }
        return null;
    }

    /**
     * 异步写入缓存
     */
    public void putAsync(CacheKey key, Object value) {
        asyncExecutor.submit(() -> {
            try {
                String json = objectMapper.writeValueAsString(value);
                redis.value(String.class).setex(
                        key.toL2Key(),
                        config.getL2TtlMinutes() * 60,
                        json);
                log.debug("[L2 Cache] Put: {}", key);
            } catch (Exception e) {
                log.warn("[L2 Cache] Put failed for key {}: {}", key, e.getMessage());
            }
        });
    }

    /**
     * 失效指定KPI和时间的缓存
     */
    public void invalidate(String kpiId, String opTime) {
        try {
            KeyCommands<String> keyCommands = redis.key();
            // 使用模式匹配查找相关的key
            String pattern = "metrics:v2:query:kpis:*" + kpiId + "*times:*" + opTime + "*";
            List<String> keys = keyCommands.keys(pattern);

            if (!keys.isEmpty()) {
                keys.forEach(keyCommands::del);
                log.info("[L2 Cache] Invalidated {} keys for kpi={}, opTime={}",
                        keys.size(), kpiId, opTime);
            }
        } catch (Exception e) {
            log.warn("[L2 Cache] Invalidation failed for kpi={}, opTime={}: {}",
                    kpiId, opTime, e.getMessage());
        }
    }

    /**
     * 清空所有缓存
     */
    public void invalidateAll() {
        try {
            KeyCommands<String> keyCommands = redis.key();
            List<String> keys = keyCommands.keys("metrics:v2:query:*");
            if (!keys.isEmpty()) {
                keys.forEach(keyCommands::del);
                log.info("[L2 Cache] Invalidated all {} keys", keys.size());
            }
        } catch (Exception e) {
            log.warn("[L2 Cache] Invalidate all failed: {}", e.getMessage());
        }
    }
}
