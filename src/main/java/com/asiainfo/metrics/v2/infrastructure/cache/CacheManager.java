package com.asiainfo.metrics.v2.infrastructure.cache;

import com.asiainfo.metrics.common.model.dto.KpiQueryRequest;
import com.asiainfo.metrics.v2.domain.model.PhysicalTableReq;
import com.asiainfo.metrics.v2.infrastructure.cache.l1.L1CaffeineCache;
import com.asiainfo.metrics.v2.infrastructure.cache.l2.L2RedisCache;
import com.asiainfo.metrics.v2.infrastructure.cache.l3.L3FileCache;
import com.fasterxml.jackson.core.type.TypeReference;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

/**
 * 统一缓存管理器
 * 提供三层缓存的统一接口
 */
@ApplicationScoped
public class CacheManager {

    private static final Logger log = LoggerFactory.getLogger(CacheManager.class);

    @Inject
    CacheConfig config;

    @Inject
    L1CaffeineCache l1Cache;

    @Inject
    L2RedisCache l2Cache;

    @Inject
    L3FileCache l3Cache;

    /**
     * 查询缓存（自动降级：L1 -> L2 -> null）
     */
    public List<Map<String, Object>> get(KpiQueryRequest req) {
        CacheKey key = CacheKey.forQuery(req);

        // L1
        if (config.isL1Enabled()) {
            @SuppressWarnings("unchecked")
            List<Map<String, Object>> result = l1Cache.get(key, List.class);
            if (result != null) {
                log.debug("[Cache] L1 hit: {}", key);
                return result;
            }
        }

        // L2
        if (config.isL2Enabled()) {
            List<Map<String, Object>> result = l2Cache.get(key, new TypeReference<>() {
            });
            if (result != null) {
                log.debug("[Cache] L2 hit: {}", key);
                // 回填L1
                if (config.isL1Enabled()) {
                    l1Cache.put(key, result);
                }
                return result;
            }
        }

        log.debug("[Cache] Miss: {}", key);
        return null;
    }

    /**
     * 写入缓存（写入所有启用的层级）
     */
    public void put(KpiQueryRequest req, List<Map<String, Object>> value) {
        CacheKey key = CacheKey.forQuery(req);

        if (config.isL1Enabled()) {
            l1Cache.put(key, value);
        }
        if (config.isL2Enabled()) {
            l2Cache.putAsync(key, value); // 异步写入
        }
    }

    /**
     * 获取文件（使用L3缓存）
     */
    public String getFile(PhysicalTableReq req) {
        try {
            return l3Cache.getOrDownload(req);
        } catch (Exception e) {
            log.error("[Cache] Failed to get file: {}", e.getMessage(), e);
            throw new RuntimeException("Failed to get cached file", e);
        }
    }

    /**
     * 失效指定KPI的所有缓存
     */
    public void invalidate(String kpiId, String opTime) {
        if (config.isL1Enabled()) {
            l1Cache.invalidate(kpiId, opTime);
        }
        if (config.isL2Enabled()) {
            l2Cache.invalidate(kpiId, opTime);
        }
        if (config.isL3Enabled()) {
            l3Cache.invalidate(kpiId, opTime);
        }
        log.info("[Cache] Invalidated: kpi={}, opTime={}", kpiId, opTime);
    }

    /**
     * 失效指定模型下所有KPI的缓存
     */
    public void invalidateModel(String kpiModelId, List<String> kpiIds, String opTime) {
        for (String kpiId : kpiIds) {
            invalidate(kpiId, opTime);
        }
        log.info("[Cache] Invalidated model: {} ({} kpis)", kpiModelId, kpiIds.size());
    }

    /**
     * 清空所有缓存
     */
    public void invalidateAll() {
        if (config.isL1Enabled()) {
            l1Cache.invalidateAll();
        }
        if (config.isL2Enabled()) {
            l2Cache.invalidateAll();
        }
        log.info("[Cache] Invalidated all");
    }

    /**
     * 获取缓存统计信息
     */
    public String getStats() {
        StringBuilder sb = new StringBuilder();
        if (config.isL1Enabled()) {
            sb.append(l1Cache.getStats()).append("\n");
        }
        return sb.toString();
    }
}
