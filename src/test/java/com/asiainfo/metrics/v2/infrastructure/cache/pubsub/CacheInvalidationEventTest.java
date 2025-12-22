package com.asiainfo.metrics.v2.infrastructure.cache.pubsub;

import com.asiainfo.metrics.infrastructure.cache.pubsub.CacheInvalidationEvent;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * CacheInvalidationEvent 单元测试
 */
class CacheInvalidationEventTest {

    @Test
    void testEventCreation() {
        CacheInvalidationEvent event = CacheInvalidationEvent.create(
                "CD002",
                List.of("KD1001", "KD1002"),
                List.of("20251201", "20251202"));

        assertNotNull(event);
        assertEquals("CD002", event.kpiModelId());
        assertEquals(2, event.kpiIds().size());
        assertEquals(2, event.opTimes().size());
        assertNotNull(event.timestamp());
        assertTrue(event.timestamp().isBefore(Instant.now().plusSeconds(1)));
    }

    @Test
    void testEventWithSingleOpTime() {
        CacheInvalidationEvent event = CacheInvalidationEvent.create(
                "CD001",
                List.of("KD1001"),
                List.of("20251201"));

        assertEquals(1, event.kpiIds().size());
        assertEquals(1, event.opTimes().size());
        assertEquals("20251201", event.opTimes().get(0));
    }
}
