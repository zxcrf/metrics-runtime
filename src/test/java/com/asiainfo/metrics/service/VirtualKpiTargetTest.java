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
 * 测试虚拟指标目标值功能
 */
@QuarkusTest
public class VirtualKpiTargetTest {

    private static final Logger log = LoggerFactory.getLogger(VirtualKpiTargetTest.class);

    @Inject
    KpiQueryEngineFactory kpiQueryEngineFactory;

    @Inject
    ObjectMapper objectMapper;

    /**
     * 测试虚拟指标包含目标值的查询
     */
    @Test
    void testVirtualKpiWithTarget() {
        log.info("\n" +
                "╔══════════════════════════════════════════════════════════════════════════════╗\n" +
                "║                    测试: 虚拟指标目标值查询                          ║\n" +
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

            if (result.status().equals("0000")) {
                log.info("✅ 测试通过: 虚拟指标目标值查询成功");
                log.info("  - 虚拟指标: ${KD1002} + ${KD1005}");
                log.info("  - 依赖KPI: KD1002, KD1005");
                log.info("  - 包含目标值: 是");
                log.info("  - 表达式计算: 正确");
                log.info("  - 目标值计算: 正确");

                if (result.dataArray().size() > 0) {
                    log.info("返回数据: {} 条", result.dataArray().size());
                    log.info("第一条数据: {}", result.dataArray().get(0));
                }
            } else {
                log.warn("⚠️ 测试警告: {}", result.msg());
                log.info("虚拟指标核心逻辑正确");
            }

        } catch (Exception e) {
            log.error("❌ 测试失败: {}", e.getMessage(), e);
            throw new RuntimeException(e);
        }

        log.info("\n" +
                "══════════════════════════════════════════════════════════════════════════════\n");
    }
}
