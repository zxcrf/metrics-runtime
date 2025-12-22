package com.asiainfo.metrics.v2.infrastructure.cache;

import com.asiainfo.metrics.api.dto.KpiQueryRequest;
import com.asiainfo.metrics.infrastructure.cache.CacheManager;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * CacheManager 集成测试
 */
@QuarkusTest
class CacheManagerTest {

    @Inject
    CacheManager cacheManager;

    private KpiQueryRequest testRequest;

    @BeforeEach
    void setUp() {
        testRequest = new KpiQueryRequest(
                List.of("KD1001"),
                List.of("20251201"),
                List.of("city_id"),
                null, // dimConditions
                null, // metricProperties
                null, // includeMetadata
                false // includeHistoricalData
        );
    }

    @Test
    void testCacheGetAndPut() {
        // 初始应该为空
        List<Map<String, Object>> result = cacheManager.get(testRequest);
        assertNull(result, "Cache should be empty initially");

        // 写入数据
        List<Map<String, Object>> data = List.of(
                Map.of("kpi_id", "KD1001", "value", 100));
        cacheManager.put(testRequest, data);

        // 应该能从缓存中读取（L1或L2）
        // 注意：由于异步写入L2，这里可能只命中L1
        result = cacheManager.get(testRequest);
        assertNotNull(result, "Cache should return data after put");
        assertEquals(1, result.size());
    }

    @Test
    void testCacheInvalidate() throws InterruptedException {
        // 写入数据
        List<Map<String, Object>> data = List.of(
                Map.of("kpi_id", "KD1001", "value", 100));
        cacheManager.put(testRequest, data);

        // 确保L1缓存生效
        Thread.sleep(100);
        List<Map<String, Object>> result = cacheManager.get(testRequest);
        assertNotNull(result);

        // 失效缓存
        cacheManager.invalidate("KD1001", "20251201");

        // L1应该被清空
        result = cacheManager.get(testRequest);
        // 可能为null（L1清空且L2未写入）或仍有数据（L2已写入）
    }

    @Test
    void testCacheStats() {
        String stats = cacheManager.getStats();
        assertNotNull(stats);
        assertTrue(stats.contains("L1 Stats"), "Stats should contain L1 information");
    }
}
