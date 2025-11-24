package com.asiainfo.metrics.service;

import com.asiainfo.metrics.model.http.KpiQueryRequest;
import com.asiainfo.metrics.model.http.KpiQueryResult;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;

@QuarkusTest
public class KpiQueryEngineTest {

    @Inject
    KpiQueryEngineFactory kpiQueryEngineFactory;

    @Inject
    ObjectMapper objectMapper;

    List<String> dimCodeArray = List.of("city_id");
    List<String> opTimeArray = List.of("20251024", "20251101");
    List<String> kpiArray = List.of("KD1008", "KD1009");
    Map<String, List<String>> testIndex = Map.of(
            "dimCodeArray", dimCodeArray,
            "opTimeArray", opTimeArray
            );
    Set<String> kpiValuesKeySet = Set.of("current", "lastCycle", "lastYear");


    @Test
    void testQuery() throws JsonProcessingException {
        String requestJson = """
                {"dimCodeArray":["city_id"],"opTimeArray":["20251024","20251101"],"kpiArray":["KD1008","KD1009"],"dimConditionArray":[],"includeHistoricalData":true,"includeTargetData":false}
                """;
        KpiQueryResult kpiQueryResult = kpiQueryEngineFactory.getQueryEngine().queryKpiData(objectMapper.readValue(requestJson, KpiQueryRequest.class));
        List<Map<String, Object>> actualDataArray = kpiQueryResult.dataArray();
        Map<String, Object> actualData = actualDataArray.getFirst();


        String exceptedJson = """
                {"dataArray":[{"kpiValues":{"KD1008":{"current":"440.9","lastYear":"--","lastCycle":"--"},"KD1009":{"current":"660.0","lastYear":"--","lastCycle":"--"}},"opTime":"20251024","city_id_desc":"北京市","city_id":"999"},{"kpiValues":{"KD1008":{"current":"1030.9","lastYear":"1760.9","lastCycle":"2134.3"},"KD1009":{"current":"1250.0","lastYear":"1980.0","lastCycle":"2575.0"}},"opTime":"20251101","city_id_desc":"北京市","city_id":"999"}]}
                """;

        KpiQueryResult expectedKpiQueryResult = objectMapper.readValue(exceptedJson, KpiQueryResult.class);
        Map<String, Object> expectedData = expectedKpiQueryResult.dataArray().getFirst();

        testIndex.forEach((k, v) -> {
            assertEquals(expectedData.get(k), actualData.get(k), "维度不同");
        });

        @SuppressWarnings("unchecked") Map<String, Map<String, Object>> expectedKpiValues = (Map<String, Map<String, Object>>) expectedData.get("kpiValues");
        @SuppressWarnings("unchecked") Map<String, Map<String, Object>> actualKpiValues = (Map<String, Map<String, Object>>) actualData.get("kpiValues");
        kpiArray.forEach(k -> {
            kpiValuesKeySet.forEach(kp -> {
                Object expectedObj = expectedKpiValues.get(k).get(kp);
                Object actualObj = actualKpiValues.get(k).get(kp);

                // 判断是否为数字类型，进行浮点数误差比较
                if (expectedObj instanceof Number && actualObj instanceof Number) {
                    double expected = ((Number) expectedObj).doubleValue();
                    double actual = ((Number) actualObj).doubleValue();
                    // 设置误差范围为1e-6，即百万分之一的误差
                    assertEquals(expected, actual, 1e-6, "指标值不同:" + k + ", kpiId:" + k + ", field:" + kp);
                } else {
                    // 非数字类型直接比较字符串
                    assertEquals(String.valueOf(expectedObj), String.valueOf(actualObj), "指标值不同:" + k + ", kpiId:" + k + ", field:" + kp);
                }
            });
        });

//        assertEquals(objectMapper.valueToTree(expectedData), objectMapper.valueToTree(actualData), "指标查询值不一致");
//        assertEquals(expectedKpiQueryResult.dataArray(), actualDataArray);
    }
}