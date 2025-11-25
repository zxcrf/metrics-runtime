package com.asiainfo.metrics.service;

import com.asiainfo.metrics.model.http.KpiQueryRequest;
import com.asiainfo.metrics.model.http.KpiQueryResult;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * 测试虚拟指标目标值功能 - 调试版
 */
@QuarkusTest
public class VirtualKpiTargetDebugTest {

    private static final Logger log = LoggerFactory.getLogger(VirtualKpiTargetDebugTest.class);

    @Inject
    KpiQueryEngineFactory kpiQueryEngineFactory;

    @Inject
    ObjectMapper objectMapper;

    /**
     * 测试虚拟指标包含目标值的查询
     */
    @Test
    void testVirtualKpiWithTargetDebug() {
        log.info("\n" +
                "╔══════════════════════════════════════════════════════════════════════════════╗\n" +
                "║                    测试: 虚拟指标目标值查询（调试版）                    ║\n" +
                "╚══════════════════════════════════════════════════════════════════════════════╝\n");

        log.info("========== 查询请求 ==========");
        String requestJson = """
                {
                    "dimCodeArray": ["city_id"],
                    "opTimeArray": ["20251024"],
                    "kpiArray": ["${KD1002} + ${KD1005}"],
                    "includeHistoricalData": false,
                    "includeTargetData": true
                }
                """;
        log.info("请求JSON: {}", requestJson);

        try {
            KpiQueryRequest request = objectMapper.readValue(requestJson, KpiQueryRequest.class);
            log.info("\n========== 执行查询 ==========");
            log.info("开始执行查询...");

            KpiQueryResult result = kpiQueryEngineFactory.getQueryEngine().queryKpiData(request);

            log.info("\n========== 查询结果 ==========");
            log.info("状态: {}", result.status());
            log.info("消息: {}", result.msg());
            log.info("返回数据条数: {}", result.dataArray().size());

            if (result.dataArray().size() > 0) {
                log.info("========== 完整数据结构 ==========");
                Map<String, Object> firstRow = result.dataArray().get(0);
                for (Map.Entry<String, Object> entry : firstRow.entrySet()) {
                    log.info("字段 {}: {}", entry.getKey(), entry.getValue());
                    if ("kpiValues".equals(entry.getKey())) {
                        @SuppressWarnings("unchecked")
                        Map<String, Map<String, Object>> kpiValues = (Map<String, Map<String, Object>>) entry.getValue();
                        for (Map.Entry<String, Map<String, Object>> kpiEntry : kpiValues.entrySet()) {
                            log.info("  KPI {}:", kpiEntry.getKey());
                            for (Map.Entry<String, Object> valueEntry : kpiEntry.getValue().entrySet()) {
                                log.info("    {}: {}", valueEntry.getKey(), valueEntry.getValue());
                            }
                        }
                    }
                }
            }

        } catch (Exception e) {
            log.error("❌ 测试失败: {}", e.getMessage(), e);
            throw new RuntimeException(e);
        }

        log.info("\n" +
                "══════════════════════════════════════════════════════════════════════════════\n");
    }
}
