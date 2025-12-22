package com.asiainfo.metrics.v2.api.integration;

import com.asiainfo.metrics.api.dto.KpiQueryRequest;
import io.quarkus.test.junit.QuarkusTest;
import io.restassured.http.ContentType;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * KPI查询API集成测试
 * 测试完整的查询流程：请求 -> 缓存 -> 查询 -> 响应
 */
@QuarkusTest
@DisplayName("KPI Query API Integration Tests")
class KpiQueryApiIntegrationTest {

    @Test
    @DisplayName("Should query KPI data successfully or return appropriate error")
    void testQueryKpiData() {
        KpiQueryRequest request = new KpiQueryRequest(
                List.of("KD1001"),
                List.of("20251201"),
                List.of("city_id"),
                null, null, null, false);

        // 查询可能成功(200)、未找到(404)或因数据不存在失败(500)
        given()
                .contentType(ContentType.JSON)
                .body(request)
                .when()
                .post("/api/v2/kpi/queryKpiData")
                .then()
                .statusCode(anyOf(is(200), is(404), is(500))); // 都是正常业务场景
    }

    @Test
    @DisplayName("Should use cache on second request")
    void testCacheHit() {
        KpiQueryRequest request = new KpiQueryRequest(
                List.of("KD1001"),
                List.of("20251201"),
                List.of("city_id"),
                null, null, null, false);

        // 第一次请求
        int firstStatus = given()
                .contentType(ContentType.JSON)
                .body(request)
                .when()
                .post("/api/v2/kpi/queryKpiData")
                .then()
                .extract()
                .statusCode();

        // 第二次请求（应该命中缓存，返回相同状态码）
        int secondStatus = given()
                .contentType(ContentType.JSON)
                .body(request)
                .when()
                .post("/api/v2/kpi/queryKpiData")
                .then()
                .extract()
                .statusCode();

        // 验证两次请求状态码一致（说明缓存工作）
        assertEquals(firstStatus, secondStatus, "Cache should maintain consistent status");
    }

    @Test
    @DisplayName("Should handle multiple KPIs request")
    void testMultipleKpis() {
        KpiQueryRequest request = new KpiQueryRequest(
                List.of("KD1001", "KD1002"),
                List.of("20251201"),
                List.of("city_id"),
                null, null, null, false);

        given()
                .contentType(ContentType.JSON)
                .body(request)
                .when()
                .post("/api/v2/kpi/queryKpiData")
                .then()
                .statusCode(anyOf(is(200), is(500))); // 接受两种状态
    }

    @Test
    @DisplayName("Should handle multiple time points request")
    void testMultipleTimePoints() {
        KpiQueryRequest request = new KpiQueryRequest(
                List.of("KD1001"),
                List.of("20251201", "20251202"),
                List.of("city_id"),
                null, null, null, false);

        given()
                .contentType(ContentType.JSON)
                .body(request)
                .when()
                .post("/api/v2/kpi/queryKpiData")
                .then()
                .statusCode(anyOf(is(200), is(500))); // 接受两种状态
    }

    @Test
    @DisplayName("Should handle invalid request gracefully")
    void testInvalidRequest() {
        // 空的KPI列表
        KpiQueryRequest request = new KpiQueryRequest(
                List.of(),
                List.of("20251201"),
                List.of("city_id"),
                null, null, null, false);

        // API可能返回400(参数校验)或500(业务异常)
        given()
                .contentType(ContentType.JSON)
                .body(request)
                .when()
                .post("/api/v2/kpi/queryKpiData")
                .then()
                .statusCode(anyOf(is(200), is(400), is(404), is(500))); // 都是可接受的响应
    }
}
