package com.asiainfo.metrics;

import com.asiainfo.metrics.service.KpiQueryEngineFactory;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * KPI计算引擎测试
 */
@QuarkusTest
class KpiSQLiteEngineTest {
    private static final Logger log = LoggerFactory.getLogger(KpiSQLiteEngineTest.class);


    @Inject
    KpiQueryEngineFactory engineFactory;

    @Inject
    ObjectMapper objectMapper;

    @Test
    void testQueryKpiDataAsync() throws JsonProcessingException {

    }
}
