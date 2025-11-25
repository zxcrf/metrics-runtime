package com.asiainfo.metrics.service;

import com.asiainfo.metrics.model.http.KpiQueryRequest;
import com.asiainfo.metrics.model.http.KpiQueryResult;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

/**
 * 真实的API SQL测试
 * 从API调用开始，实际执行查询并打印生成的SQL
 */
@QuarkusTest
public class RealApiSqlTest {

    private static final Logger log = LoggerFactory.getLogger(RealApiSqlTest.class);

    @Inject
    KpiQueryEngineFactory kpiQueryEngineFactory;

    @Inject
    ObjectMapper objectMapper;

    /**
     * 测试1：实际API调用 - 简单派生指标查询
     * 这个测试会真正执行查询并打印生成的SQL
     */
    @Test
    void testRealApiQuery_ExtendedKpi() {
        log.info("\n" +
                "╔══════════════════════════════════════════════════════════════════════════════╗\n" +
                "║                    测试1: 真实API调用 - 派生指标查询                      ║\n" +
                "╚══════════════════════════════════════════════════════════════════════════════╝\n");

        log.info("========== 查询请求 ==========");
        String requestJson = """
                {
                    "dimCodeArray": ["city_id"],
                    "opTimeArray": ["20251024"],
                    "kpiArray": ["KD1008"],
                    "dimConditionArray": [],
                    "includeHistoricalData": false,
                    "includeTargetData": false
                }
                """;
        log.info("请求JSON: {}", requestJson);

        try {
            KpiQueryRequest request = objectMapper.readValue(requestJson, KpiQueryRequest.class);
            log.info("\n========== 执行查询 ==========");
            log.info("开始执行查询...");

            // 执行真正的查询
            KpiQueryResult result = kpiQueryEngineFactory.getQueryEngine().queryKpiData(request);

            log.info("\n========== 查询结果 ==========");
            log.info("状态: {}", result.status());
            log.info("消息: {}", result.msg());
            log.info("返回数据条数: {}", result.dataArray().size());

            if (result.dataArray().size() > 0) {
                log.info("第一条数据: {}", result.dataArray().get(0));
            }

            // 验证结果
            assert result.status().equals("0000") : "查询应该成功";
            log.info("✅ 测试1通过: 派生指标查询成功");

        } catch (Exception e) {
            log.error("❌ 测试1失败: {}", e.getMessage(), e);
            throw new RuntimeException(e);
        }

        log.info("\n" +
                "══════════════════════════════════════════════════════════════════════════════\n");
    }

    /**
     * 测试2：实际API调用 - 多个KPI查询
     */
    @Test
    void testRealApiQuery_MultipleKpis() {
        log.info("\n" +
                "╔══════════════════════════════════════════════════════════════════════════════╗\n" +
                "║                    测试2: 真实API调用 - 多KPI查询                        ║\n" +
                "╚══════════════════════════════════════════════════════════════════════════════╝\n");

        log.info("========== 查询请求 ==========");
        String requestJson = """
                {
                    "dimCodeArray": ["city_id"],
                    "opTimeArray": ["20251024"],
                    "kpiArray": ["KD1008", "KD1009"],
                    "dimConditionArray": [],
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

            assert result.status().equals("0000") : "查询应该成功";
            log.info("✅ 测试2通过: 多KPI查询成功");

        } catch (Exception e) {
            log.error("❌ 测试2失败: {}", e.getMessage(), e);
            throw new RuntimeException(e);
        }

        log.info("\n" +
                "══════════════════════════════════════════════════════════════════════════════\n");
    }

    /**
     * 测试3：实际API调用 - 包含历史数据
     */
    @Test
    void testRealApiQuery_WithHistoricalData() {
        log.info("\n" +
                "╔══════════════════════════════════════════════════════════════════════════════╗\n" +
                "║                 测试3: 真实API调用 - 包含历史数据                     ║\n" +
                "╚══════════════════════════════════════════════════════════════════════════════╝\n");

        log.info("========== 查询请求 ==========");
        String requestJson = """
                {
                    "dimCodeArray": ["city_id"],
                    "opTimeArray": ["20251101"],
                    "kpiArray": ["KD1008"],
                    "dimConditionArray": [],
                    "includeHistoricalData": true,
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

            assert result.status().equals("0000") : "查询应该成功";
            log.info("✅ 测试3通过: 历史数据查询成功");

        } catch (Exception e) {
            log.error("❌ 测试3失败: {}", e.getMessage(), e);
            throw new RuntimeException(e);
        }

        log.info("\n" +
                "══════════════════════════════════════════════════════════════════════════════\n");
    }

    /**
     * 测试4：实际API调用 - 计算指标
     */
    @Test
    void testRealApiQuery_ComputedKpi() {
        log.info("\n" +
                "╔══════════════════════════════════════════════════════════════════════════════╗\n" +
                "║                    测试4: 真实API调用 - 计算指标                       ║\n" +
                "╚══════════════════════════════════════════════════════════════════════════════╝\n");

        log.info("========== 查询请求 ==========");
        String requestJson = """
                {
                    "dimCodeArray": ["city_id"],
                    "opTimeArray": ["20251024"],
                    "kpiArray": ["KD2001"],
                    "dimConditionArray": [],
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

            assert result.status().equals("0000") : "查询应该成功";
            log.info("✅ 测试4通过: 计算指标查询成功");

        } catch (Exception e) {
            log.error("❌ 测试4失败: {}", e.getMessage(), e);
            throw new RuntimeException(e);
        }

        log.info("\n" +
                "══════════════════════════════════════════════════════════════════════════════\n");
    }

    /**
     * 测试5：实际API调用 - 复杂表达式
     */
    @Test
    void testRealApiQuery_ComplexExpression() {
        log.info("\n" +
                "╔══════════════════════════════════════════════════════════════════════════════╗\n" +
                "║                 测试5: 真实API调用 - 复杂表达式                       ║\n" +
                "╚══════════════════════════════════════════════════════════════════════════════╝\n");

        log.info("========== 查询请求 ==========");
        String requestJson = """
                {
                    "dimCodeArray": ["city_id"],
                    "opTimeArray": ["20251101"],
                    "kpiArray": ["(${KD1003}-${KD1003.lastYear})/${KD1003.lastYear}"],
                    "dimConditionArray": [],
                    "includeHistoricalData": true,
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

            assert result.status().equals("0000") : "查询应该成功";
            log.info("✅ 测试5通过: 复杂表达式查询成功");

        } catch (Exception e) {
            log.error("❌ 测试5失败: {}", e.getMessage(), e);
            throw new RuntimeException(e);
        }

        log.info("\n" +
                "══════════════════════════════════════════════════════════════════════════════\n");
    }
}
