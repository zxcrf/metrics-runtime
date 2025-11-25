package com.asiainfo.metrics.service;

import com.asiainfo.metrics.model.http.KpiQueryRequest;
import com.asiainfo.metrics.model.http.KpiQueryResult;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * SQL验证测试
 * 专门用来打印和验证生成的SQL语句是否正确
 */
@QuarkusTest
public class SqlValidationTest {

    private static final Logger log = LoggerFactory.getLogger(SqlValidationTest.class);

    @Inject
    KpiQueryEngineFactory kpiQueryEngineFactory;

    @Inject
    ObjectMapper objectMapper;

    /**
     * 测试1：简单派生指标查询
     * KD1008和KD1009都是派生指标，使用sum聚合
     */
    @Test
    void testExtendedKpiQuery() throws JsonProcessingException {
        log.info("\n" +
                "================================================================================\n" +
                "【测试1】简单派生指标查询 (Extended KPI)\n" +
                "================================================================================\n" +
                "KPI: KD1008, KD1009 (都是派生指标，类型：extended)\n" +
                "聚合函数: sum\n" +
                "维度: city_id\n" +
                "时间: 20251024\n" +
                "================================================================================\n");

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

        KpiQueryRequest request = objectMapper.readValue(requestJson, KpiQueryRequest.class);
        KpiQueryResult result = kpiQueryEngineFactory.getQueryEngine().queryKpiData(request);

        log.info("查询结果状态: {}", result.status());
        log.info("查询结果消息: {}", result.msg());
        log.info("返回数据条数: {}", result.dataArray().size());

        // 验证结果
        assert result.status().equals("0000") : "查询应该成功";
        assert result.dataArray().size() > 0 : "应该有返回数据";
    }

    /**
     * 测试2：复合指标查询
     * KD2001是计算指标，表达式为KD1002 + KD1005
     */
    @Test
    void testComputedKpiQuery() throws JsonProcessingException {
        log.info("\n" +
                "================================================================================\n" +
                "【测试2】复合指标查询 (Computed KPI)\n" +
                "================================================================================\n" +
                "KPI: KD2001 (复合指标，类型：composite，计算方法：expr)\n" +
                "表达式: KD1002 + KD1005\n" +
                "聚合函数: sum\n" +
                "维度: city_id\n" +
                "时间: 20251024\n" +
                "================================================================================\n");

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

        KpiQueryRequest request = objectMapper.readValue(requestJson, KpiQueryRequest.class);
        KpiQueryResult result = kpiQueryEngineFactory.getQueryEngine().queryKpiData(request);

        log.info("查询结果状态: {}", result.status());
        log.info("查询结果消息: {}", result.msg());
        log.info("返回数据条数: {}", result.dataArray().size());

        // 验证结果
        assert result.status().equals("0000") : "查询应该成功";
    }

    /**
     * 测试3：包含历史数据的查询
     * 查询当前周期、上一周期、去年同期的数据
     */
    @Test
    void testHistoricalDataQuery() throws JsonProcessingException {
        log.info("\n" +
                "================================================================================\n" +
                "【测试3】包含历史数据的查询\n" +
                "================================================================================\n" +
                "KPI: KD1008, KD1009\n" +
                "时间点: 20251101\n" +
                "历史数据: lastCycle (20251001), lastYear (20231101)\n" +
                "includeHistoricalData: true\n" +
                "================================================================================\n");

        String requestJson = """
                {
                    "dimCodeArray": ["city_id"],
                    "opTimeArray": ["20251101"],
                    "kpiArray": ["KD1008", "KD1009"],
                    "dimConditionArray": [],
                    "includeHistoricalData": true,
                    "includeTargetData": false
                }
                """;

        KpiQueryRequest request = objectMapper.readValue(requestJson, KpiQueryRequest.class);
        KpiQueryResult result = kpiQueryEngineFactory.getQueryEngine().queryKpiData(request);

        log.info("查询结果状态: {}", result.status());
        log.info("查询结果消息: {}", result.msg());
        log.info("返回数据条数: {}", result.dataArray().size());

        // 验证结果
        assert result.status().equals("0000") : "查询应该成功";
    }

    /**
     * 测试4：复杂表达式查询
     * 使用${}语法和时间修饰符
     */
    @Test
    void testComplexExpressionQuery() throws JsonProcessingException {
        log.info("\n" +
                "================================================================================\n" +
                "【测试4】复杂表达式查询 (Virtual KPI)\n" +
                "================================================================================\n" +
                "表达式: ${KD1003.lastYear} + ${KD1003}\n" +
                "说明: 去年同期 + 当前周期\n" +
                "时间修饰符: .lastYear (去年同期)\n" +
                "时间: 20251101\n" +
                "================================================================================\n");

        String requestJson = """
                {
                    "dimCodeArray": ["city_id"],
                    "opTimeArray": ["20251101"],
                    "kpiArray": ["${KD1003.lastYear} + ${KD1003}"],
                    "dimConditionArray": [],
                    "includeHistoricalData": true,
                    "includeTargetData": false
                }
                """;

        KpiQueryRequest request = objectMapper.readValue(requestJson, KpiQueryRequest.class);
        KpiQueryResult result = kpiQueryEngineFactory.getQueryEngine().queryKpiData(request);

        log.info("查询结果状态: {}", result.status());
        log.info("查询结果消息: {}", result.msg());
        log.info("返回数据条数: {}", result.dataArray().size());

        // 验证结果
        assert result.status().equals("0000") : "查询应该成功";
    }

    /**
     * 测试5：同比增长率计算
     * (当前值 - 去年同期) / 去年同期
     */
    @Test
    void testYearOverYearGrowthQuery() throws JsonProcessingException {
        log.info("\n" +
                "================================================================================\n" +
                "【测试5】同比增长率计算\n" +
                "================================================================================\n" +
                "表达式: (${KD1003}-${KD1003.lastYear})/${KD1003.lastYear}\n" +
                "说明: (当前值 - 去年同期) / 去年同期\n" +
                "时间修饰符: .lastYear (去年同期)\n" +
                "时间: 20251101\n" +
                "================================================================================\n");

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

        KpiQueryRequest request = objectMapper.readValue(requestJson, KpiQueryRequest.class);
        KpiQueryResult result = kpiQueryEngineFactory.getQueryEngine().queryKpiData(request);

        log.info("查询结果状态: {}", result.status());
        log.info("查询结果消息: {}", result.msg());
        log.info("返回数据条数: {}", result.dataArray().size());

        // 验证结果
        assert result.status().equals("0000") : "查询应该成功";
    }

    /**
     * 测试6：多个KPI批量查询
     */
    @Test
    void testMultipleKpiQuery() throws JsonProcessingException {
        log.info("\n" +
                "================================================================================\n" +
                "【测试6】多个KPI批量查询\n" +
                "================================================================================\n" +
                "KPI: KD1008, KD1009, KD2001\n" +
                "说明: 包含派生指标和计算指标\n" +
                "时间: 20251024, 20251101\n" +
                "================================================================================\n");

        String requestJson = """
                {
                    "dimCodeArray": ["city_id"],
                    "opTimeArray": ["20251024", "20251101"],
                    "kpiArray": ["KD1008", "KD1009", "KD2001"],
                    "dimConditionArray": [],
                    "includeHistoricalData": false,
                    "includeTargetData": false
                }
                """;

        KpiQueryRequest request = objectMapper.readValue(requestJson, KpiQueryRequest.class);
        KpiQueryResult result = kpiQueryEngineFactory.getQueryEngine().queryKpiData(request);

        log.info("查询结果状态: {}", result.status());
        log.info("查询结果消息: {}", result.msg());
        log.info("返回数据条数: {}", result.dataArray().size());

        // 验证结果
        assert result.status().equals("0000") : "查询应该成功";
    }

    /**
     * 测试7：无维度的查询（按时间汇总）
     */
    @Test
    void testNoDimensionQuery() throws JsonProcessingException {
        log.info("\n" +
                "================================================================================\n" +
                "【测试7】无维度的查询（按时间汇总）\n" +
                "================================================================================\n" +
                "KPI: KD1008, KD1009\n" +
                "维度: 无（按时间汇总）\n" +
                "时间: 20251024\n" +
                "================================================================================\n");

        String requestJson = """
                {
                    "dimCodeArray": [],
                    "opTimeArray": ["20251024"],
                    "kpiArray": ["KD1008", "KD1009"],
                    "dimConditionArray": [],
                    "includeHistoricalData": false,
                    "includeTargetData": false
                }
                """;

        KpiQueryRequest request = objectMapper.readValue(requestJson, KpiQueryRequest.class);
        KpiQueryResult result = kpiQueryEngineFactory.getQueryEngine().queryKpiData(request);

        log.info("查询结果状态: {}", result.status());
        log.info("查询结果消息: {}", result.msg());
        log.info("返回数据条数: {}", result.dataArray().size());

        // 验证结果
        assert result.status().equals("0000") : "查询应该成功";
    }

    /**
     * 测试8：维度条件过滤查询
     */
    @Test
    void testDimensionFilterQuery() throws JsonProcessingException {
        log.info("\n" +
                "================================================================================\n" +
                "【测试8】维度条件过滤查询\n" +
                "================================================================================\n" +
                "KPI: KD1008\n" +
                "维度: city_id\n" +
                "过滤条件: city_id IN (999)\n" +
                "时间: 20251024\n" +
                "================================================================================\n");

        String requestJson = """
                {
                    "dimCodeArray": ["city_id"],
                    "opTimeArray": ["20251024"],
                    "kpiArray": ["KD1008"],
                    "dimConditionArray": [{"dimConditionCode": "city_id", "dimConditionVal": "999"}],
                    "includeHistoricalData": false,
                    "includeTargetData": false
                }
                """;

        KpiQueryRequest request = objectMapper.readValue(requestJson, KpiQueryRequest.class);
        KpiQueryResult result = kpiQueryEngineFactory.getQueryEngine().queryKpiData(request);

        log.info("查询结果状态: {}", result.status());
        log.info("查询结果消息: {}", result.msg());
        log.info("返回数据条数: {}", result.dataArray().size());

        // 验证结果
        assert result.status().equals("0000") : "查询应该成功";
    }

    /**
     * 辅助方法：打印测试开始信息
     */
    private void printTestHeader(String testName, String description) {
        log.info("\n" +
                "╔════════════════════════════════════════════════════════════════════════════════╗\n" +
                "║                          {}                                      ║\n" +
                "╚════════════════════════════════════════════════════════════════════════════════╝\n",
                testName);
        log.info("说明: {}", description);
    }

    /**
     * 辅助方法：打印测试结束信息
     */
    private void printTestFooter(String testName, boolean success) {
        log.info("\n" +
                "【{}】测试 {}\n" +
                "════════════════════════════════════════════════════════════════════════════════\n",
                testName,
                success ? "通过 ✅" : "失败 ❌");
    }
}
