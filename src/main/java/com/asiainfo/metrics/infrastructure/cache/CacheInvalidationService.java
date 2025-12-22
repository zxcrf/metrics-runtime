package com.asiainfo.metrics.infrastructure.cache;

import com.asiainfo.metrics.infrastructure.cache.pubsub.CacheInvalidationPublisher;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * 缓存失效服务
 * 提供统一的缓存失效接口，支持本地和集群失效
 * 
 * 使用精确的 kpiId + opTime 组合失效，避免缓存雪崩
 */
@ApplicationScoped
public class CacheInvalidationService {

    private static final Logger log = LoggerFactory.getLogger(CacheInvalidationService.class);

    @Inject
    CacheManager cacheManager;

    @Inject
    CacheInvalidationPublisher publisher;

    /**
     * 失效单个KPI在指定时间点的缓存（仅本地）
     */
    public void invalidateKpi(String kpiId, String opTime) {
        cacheManager.invalidate(kpiId, opTime);
        log.info("[Cache Invalidation] Local invalidation: kpi={}, opTime={}", kpiId, opTime);
    }

    /**
     * 失效模型下所有KPI在指定时间点的缓存，并发布到集群
     * 
     * @param kpiModelId 模型ID
     * @param kpiIds     模型下所有派生指标ID列表
     * @param opTimes    要失效的时间点列表
     */
    public void invalidateModelAndPublish(String kpiModelId, List<String> kpiIds, List<String> opTimes) {
        // 1. 本地失效 - 精确失效每个 kpiId + opTime 组合
        for (String kpiId : kpiIds) {
            for (String opTime : opTimes) {
                cacheManager.invalidate(kpiId, opTime);
            }
        }

        // 2. 发布到集群其他节点
        publisher.publishModelInvalidation(kpiModelId, kpiIds, opTimes);

        log.info("[Cache Invalidation] Model invalidated and published: model={}, kpis={}, opTimes={}",
                kpiModelId, kpiIds.size(), opTimes);
    }

    /**
     * 失效单个KPI在多个时间点的缓存并发布
     */
    public void invalidateKpiAndPublish(String kpiId, List<String> opTimes) {
        for (String opTime : opTimes) {
            invalidateKpi(kpiId, opTime);
        }
        publisher.publishKpiInvalidation(kpiId, opTimes);
    }
}
