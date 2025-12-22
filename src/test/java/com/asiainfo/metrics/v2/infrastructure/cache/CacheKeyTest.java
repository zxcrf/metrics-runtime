package com.asiainfo.metrics.v2.infrastructure.cache;

import com.asiainfo.metrics.api.dto.KpiQueryRequest;
import com.asiainfo.metrics.infrastructure.cache.CacheKey;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * CacheKey 单元测试
 */
class CacheKeyTest {

    @Test
    void testCacheKeyGeneration() {
        var req = new KpiQueryRequest(
                List.of("KD1001", "KD1002"),
                List.of("20251201", "20251202"),
                List.of("city_id"),
                null, null, null, false);

        CacheKey key = CacheKey.forQuery(req);

        String l1Key = key.toL1Key();
        assertNotNull(l1Key);
        assertTrue(l1Key.contains("metrics:v2:query:"));
        assertTrue(l1Key.contains("kpis:"));
        assertTrue(l1Key.contains("times:"));
        assertTrue(l1Key.contains("dims:"));
        assertTrue(l1Key.contains("hist:false"));
    }

    @Test
    void testCacheKeySorting() {
        // 不同顺序的参数应该生成相同的key
        var req1 = new KpiQueryRequest(
                List.of("KD1001", "KD1002"),
                List.of("20251201"),
                List.of("city_id"),
                null, null, null, false);

        var req2 = new KpiQueryRequest(
                List.of("KD1002", "KD1001"), // 顺序不同
                List.of("20251201"),
                List.of("city_id"),
                null, null, null, false);

        CacheKey key1 = CacheKey.forQuery(req1);
        CacheKey key2 = CacheKey.forQuery(req2);

        assertEquals(key1.toL1Key(), key2.toL1Key(), "Keys should be same regardless of input order");
    }

    @Test
    void testFileKey() {
        CacheKey key = CacheKey.forFile("KD1001", "20251201", "city_id");

        assertTrue(key.isFileType());
        assertEquals(List.of("KD1001"), key.getKpiIds());
        assertEquals(List.of("20251201"), key.getOpTimes());
    }
}
