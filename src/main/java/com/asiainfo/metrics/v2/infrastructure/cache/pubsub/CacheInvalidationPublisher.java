package com.asiainfo.metrics.v2.infrastructure.cache.pubsub;

import com.asiainfo.metrics.v2.infrastructure.cache.CacheConfig;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.quarkus.redis.datasource.RedisDataSource;
import io.quarkus.redis.datasource.pubsub.PubSubCommands;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * 缓存失效发布者
 * 向Redis Pub/Sub发布缓存失效事件，通知所有节点
 */
@ApplicationScoped
public class CacheInvalidationPublisher {

    private static final Logger log = LoggerFactory.getLogger(CacheInvalidationPublisher.class);

    @Inject
    CacheConfig config;

    @Inject
    RedisDataSource redis;

    @Inject
    ObjectMapper objectMapper;

    /**
     * 发布模型级缓存失效事件
     * 
     * @param kpiModelId 模型ID
     * @param kpiIds     该模型下的所有KPI ID列表
     * @param opTimes    要失效的时间点列表
     */
    public void publishModelInvalidation(String kpiModelId, List<String> kpiIds, List<String> opTimes) {
        if (!config.isInvalidationEnabled()) {
            log.debug("[Cache Invalidation] Disabled, skipping publish");
            return;
        }

        try {
            CacheInvalidationEvent event = CacheInvalidationEvent.create(kpiModelId, kpiIds, opTimes);
            String message = objectMapper.writeValueAsString(event);

            PubSubCommands<String> pubsub = redis.pubsub(String.class);
            pubsub.publish(config.getInvalidationChannel(), message);

            log.info("[Cache Invalidation] Published for model={}, kpis={}, opTimes={}",
                    kpiModelId, kpiIds.size(), opTimes);
        } catch (Exception e) {
            log.error("[Cache Invalidation] Failed to publish: {}", e.getMessage(), e);
        }
    }

    /**
     * 发布单个KPI的缓存失效
     */
    public void publishKpiInvalidation(String kpiId, List<String> opTimes) {
        publishModelInvalidation("single", List.of(kpiId), opTimes);
    }
}
