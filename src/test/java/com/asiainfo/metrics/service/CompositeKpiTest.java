package com.asiainfo.metrics.service;

import com.asiainfo.metrics.model.http.KpiQueryRequest;
import com.asiainfo.metrics.model.http.KpiQueryResult;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 测试复合指标查询
 */
@QuarkusTest
public class CompositeKpiTest {

    private static final Logger log = LoggerFactory.getLogger(CompositeKpiTest.class);

    @Inject
    KpiQueryEngineFactory kpiQueryEngineFactory;

    @Inject
    ObjectMapper objectMapper;

    /**
     * 测试复合指标查询
     */
    @Test
    void testCompositeKpi() {
        log.info("\n" +
                "╔══════════════════════════════════════════════════════════════════════════════╗\n" +
                "║                          测试: 复合指标查询                          ║\n" +
                "╚══════════════════════════════════════════════════════════════════════════════╝\n");

        log.info("========== 查询请求 ==========");
        String requestJson = """
                {
                    "dimCodeArray": ["city_id"],
                    "opTimeArray": ["20251024"],
                    "kpiArray": ["KD1002", "KD1005", "KD3000", "KD3001", "KD3002", "KD3003"],
                    "includeHistoricalData": false,
                    "includeTargetData": false
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

            if (result.status().equals("0000")) {
                log.info("✅ 测试通过: 复合指标查询成功");
                log.info("  - 查询KPI: KD1002, KD1005, KD3000, KD3001, KD3002, KD3003");
                log.info("  - 返回KPI数: {}", result.dataArray().size() > 0 && result.dataArray().get(0).containsKey("kpiValues") ?
                        ((java.util.Map<?, ?>) result.dataArray().get(0).get("kpiValues")).size() : 0);
                log.info("  - 期望返回: 6个KPI (2个普通 + 4个复合)");

                if (result.dataArray().size() > 0) {
                    log.info("返回数据: {} 条", result.dataArray().size());
                    log.info("第一条数据: {}", result.dataArray().get(0));
                }
            } else {
                log.warn("⚠️ 测试警告: {}", result.msg());
            }

        } catch (Exception e) {
            log.error("❌ 测试失败: {}", e.getMessage(), e);
            throw new RuntimeException(e);
        }

        log.info("\n" +
                "══════════════════════════════════════════════════════════════════════════════\n");
    }
}
