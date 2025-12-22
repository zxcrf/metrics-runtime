package com.asiainfo.metrics.v2.infrastructure.cache;

import com.asiainfo.metrics.infrastructure.cache.CacheConfig;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * CacheConfig 单元测试
 */
@QuarkusTest
class CacheConfigTest {

    @Inject
    CacheConfig config;

    @Test
    void testCacheConfigLoaded() {
        assertNotNull(config);

        // 默认配置应该都启用
        assertTrue(config.isL1Enabled(), "L1 cache should be enabled by default");
        assertTrue(config.isL2Enabled(), "L2 cache should be enabled by default");
        assertTrue(config.isL3Enabled(), "L3 cache should be enabled by default");

        // 检查配置值
        assertEquals(5, config.getL1TtlSeconds());
        assertEquals(1000, config.getL1MaxSize());
        assertEquals(30, config.getL2TtlMinutes());
        assertEquals(10240, config.getL3MaxSizeMb());

        // 检查失效配置
        assertTrue(config.isInvalidationEnabled());
        assertEquals("cache:invalidation", config.getInvalidationChannel());
    }
}
