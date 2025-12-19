package com.asiainfo.metrics.v2.infrastructure.cache.pubsub;

import com.asiainfo.metrics.v2.infrastructure.cache.CacheConfig;
import com.asiainfo.metrics.v2.infrastructure.cache.CacheManager;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.quarkus.redis.datasource.RedisDataSource;
import io.quarkus.runtime.Startup;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * 缓存失效订阅者
 * 监听Redis Pub/Sub频道，处理缓存失效事件
 */
@Startup
@ApplicationScoped
public class CacheInvalidationSubscriber {

    private static final Logger log = LoggerFactory.getLogger(CacheInvalidationSubscriber.class);

    @Inject
    CacheConfig config;

    @Inject
    CacheManager cacheManager;

    @Inject
    RedisDataSource redis;

    @Inject
    ObjectMapper objectMapper;

    private final ExecutorService subscriptionExecutor = Executors.newVirtualThreadPerTaskExecutor();

    @PostConstruct
    void init() {
        if (!config.isInvalidationEnabled()) {
            log.info("[Cache Invalidation] Subscription disabled");
            return;
        }

        // 异步启动订阅，避免阻塞应用启动
        subscriptionExecutor.submit(this::startSubscription);
    }

    private void startSubscription() {
        try {
            String channel = config.getInvalidationChannel();
            log.info("[Cache Invalidation] Subscribing to channel: {}", channel);

            redis.pubsub(String.class).subscribe(channel, message -> {
                handleInvalidation(message);
            });

        } catch (Exception e) {
            log.error("[Cache Invalidation] Subscription failed: {}", e.getMessage(), e);
        }
    }

    private void handleInvalidation(String message) {
        try {
            CacheInvalidationEvent event = objectMapper.readValue(message, CacheInvalidationEvent.class);

            log.info("[Cache Invalidation] Received: model={}, kpis={}, opTimes={}",
                    event.kpiModelId(), event.kpiIds(), event.opTimes());

            // 对每个 kpiId + opTime 组合进行精确失效
            for (String kpiId : event.kpiIds()) {
                for (String opTime : event.opTimes()) {
                    cacheManager.invalidate(kpiId, opTime);
                }
            }

            log.info("[Cache Invalidation] Invalidated {} kpis x {} opTimes = {} combinations",
                    event.kpiIds().size(), event.opTimes().size(),
                    event.kpiIds().size() * event.opTimes().size());

        } catch (Exception e) {
            log.error("[Cache Invalidation] Failed to handle message: {}", e.getMessage(), e);
        }
    }

    @PreDestroy
    void destroy() {
        subscriptionExecutor.shutdownNow();
    }
}
