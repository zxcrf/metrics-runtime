package com.asiainfo.metrics.v2;

import com.asiainfo.metrics.model.http.KpiQueryRequest;
import com.asiainfo.metrics.v2.core.engine.UnifiedMetricEngine;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 测试v2引擎
 * 验证新的架构是否能正确处理复合指标和虚拟指标
 */
@QuarkusTest
public class V2EngineTest {

    private static final Logger log = LoggerFactory.getLogger(V2EngineTest.class);

    @Inject
    UnifiedMetricEngine engine;

    /**
     * 测试复合指标查询
     */
    @Test
    void testCompositeKpi() {
        log.info("\n" +
                "╔══════════════════════════════════════════════════════════════════════════════╗\n" +
                "║                         测试: v2引擎复合指标查询                          ║\n" +
                "╚══════════════════════════════════════════════════════════════════════════════╝\n");

        log.info("========== 查询请求 ==========");
        List<String> kpiIds = List.of("KD1002", "KD1005", "KD3000", "KD3001", "KD3002", "KD3003");
        String opTime = "20251024";
        List<String> dims = List.of("city_id");

        log.info("KPI IDs: {}", kpiIds);
        log.info("OpTime: {}", opTime);
        log.info("Dimensions: {}", dims);

        log.info("\n========== 执行查询 ==========");
        KpiQueryRequest req = new KpiQueryRequest(kpiIds, List.of(opTime), dims, List.of(), new HashMap<>(), false, false);
        List<Map<String, Object>> results = engine.execute(req);

        log.info("\n========== 查询结果 ==========");
        log.info("返回数据条数: {}", results.size());

        if (!results.isEmpty()) {
            Map<String, Object> firstRow = results.get(0);
            log.info("第一条数据: {}", firstRow);

            if (firstRow.containsKey("kpiValues")) {
                @SuppressWarnings("unchecked")
                Map<String, Map<String, Object>> kpiValues =
                        (Map<String, Map<String, Object>>) firstRow.get("kpiValues");
                log.info("KPI数量: {}", kpiValues.size());
                for (String kpiId : kpiValues.keySet()) {
                    log.info("  {}: {}", kpiId, kpiValues.get(kpiId));
                }
            }
        }

        log.info("\n" +
                "══════════════════════════════════════════════════════════════════════════════\n");
    }
}
