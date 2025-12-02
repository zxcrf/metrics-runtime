package com.asiainfo.metrics.v2.infrastructure.cache;

import jakarta.annotation.PostConstruct;
import jakarta.enterprise.context.ApplicationScoped;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 缓存配置管理
 * 统一管理三层缓存的配置项
 */
@ApplicationScoped
public class CacheConfig {

    private static final Logger log = LoggerFactory.getLogger(CacheConfig.class);

    // L1 Caffeine配置
    @ConfigProperty(name = "kpi.cache.l1.enabled", defaultValue = "true")
    boolean l1Enabled;

    @ConfigProperty(name = "kpi.cache.l1.ttl-seconds", defaultValue = "5")
    int l1TtlSeconds;

    @ConfigProperty(name = "kpi.cache.l1.max-size", defaultValue = "1000")
    int l1MaxSize;

    // L2 Redis配置
    @ConfigProperty(name = "kpi.cache.l2.enabled", defaultValue = "true")
    boolean l2Enabled;

    @ConfigProperty(name = "kpi.cache.l2.ttl-minutes", defaultValue = "30")
    int l2TtlMinutes;

    // L3 文件缓存配置
    @ConfigProperty(name = "kpi.cache.l3.enabled", defaultValue = "true")
    boolean l3Enabled;

    @ConfigProperty(name = "kpi.cache.l3.max-size-mb", defaultValue = "10240")
    long l3MaxSizeMb;

    @ConfigProperty(name = "kpi.cache.l3.cleanup-interval-minutes", defaultValue = "60")
    long l3CleanupIntervalMinutes;

    // 缓存失效配置
    @ConfigProperty(name = "kpi.cache.invalidation.channel", defaultValue = "cache:invalidation")
    String invalidationChannel;

    @ConfigProperty(name = "kpi.cache.invalidation.enabled", defaultValue = "true")
    boolean invalidationEnabled;

    @PostConstruct
    void init() {
        log.info("=== Cache Configuration ===");
        log.info("L1 (Caffeine): {} (TTL: {}s, MaxSize: {})",
                l1Enabled ? "ENABLED" : "DISABLED", l1TtlSeconds, l1MaxSize);
        log.info("L2 (Redis):    {} (TTL: {}min)",
                l2Enabled ? "ENABLED" : "DISABLED", l2TtlMinutes);
        log.info("L3 (File):     {} (MaxSize: {}MB)",
                l3Enabled ? "ENABLED" : "DISABLED", l3MaxSizeMb);
        log.info("Invalidation:  {} (Channel: {})",
                invalidationEnabled ? "ENABLED" : "DISABLED", invalidationChannel);
        log.info("===========================");
    }

    // Getters
    public boolean isL1Enabled() {
        return l1Enabled;
    }

    public int getL1TtlSeconds() {
        return l1TtlSeconds;
    }

    public int getL1MaxSize() {
        return l1MaxSize;
    }

    public boolean isL2Enabled() {
        return l2Enabled;
    }

    public int getL2TtlMinutes() {
        return l2TtlMinutes;
    }

    public boolean isL3Enabled() {
        return l3Enabled;
    }

    public long getL3MaxSizeMb() {
        return l3MaxSizeMb;
    }

    public long getL3CleanupIntervalMinutes() {
        return l3CleanupIntervalMinutes;
    }

    public String getInvalidationChannel() {
        return invalidationChannel;
    }

    public boolean isInvalidationEnabled() {
        return invalidationEnabled;
    }
}
