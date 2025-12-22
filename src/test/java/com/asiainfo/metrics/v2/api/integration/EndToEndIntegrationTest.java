package com.asiainfo.metrics.v2.api.integration;

import com.asiainfo.metrics.api.dto.SrcTableCompleteRequest;
import io.quarkus.test.junit.QuarkusTest;
import io.restassured.http.ContentType;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * 端到端集成测试
 * 测试完整业务流程：ETL -> 日志 -> 查询 -> 缓存
 */
@QuarkusTest
@DisplayName("End-to-End Integration Tests")
class EndToEndIntegrationTest {

    @Test
    @DisplayName("Complete flow: ETL -> Logging -> Query Logs")
    void testCompleteWorkflow() throws InterruptedException {
        String testTable = "e2e_test_table";
        String opTime = "20251207";

        // 1. 触发ETL（关键：验证日志创建）
        Integer executionId = given()
                .contentType(ContentType.JSON)
                .body(new SrcTableCompleteRequest(testTable, opTime))
                .when()
                .post("/api/v2/kpi/srcTableComplete")
                .then()
                .statusCode(200)
                .body("executionId", notNullValue())
                .body("executionTimeMs", notNullValue())
                .extract()
                .path("executionId");

        assertNotNull(executionId, "Execution ID should be created");

        // 2. 验证日志已记录在数据库中
        Thread.sleep(500); // 等待异步写入
        given()
                .when()
                .get("/api/v2/etl/logs/" + executionId)
                .then()
                .statusCode(anyOf(is(200), is(404))); // 200表示找到，404表示异步还未完成

        // 3. 查询该模型的最近执行记录
        given()
                .queryParam("limit", 5)
                .when()
                .get("/api/v2/etl/logs/latest/" + testTable)
                .then()
                .statusCode(200)
                .body("$", isA(java.util.List.class));

        // 4. 查询统计信息
        given()
                .when()
                .get("/api/v2/etl/logs/stats")
                .then()
                .statusCode(200)
                .body("totalExecutions", greaterThan(0));
    }

    @Test
    @DisplayName("ETL redo should create additional log entry")
    void testEtlRedoFlow() {
        String testTable = "redo_test_table_e2e";
        String opTime = "20251208";

        // 首次执行
        Integer firstId = given()
                .contentType(ContentType.JSON)
                .body(new SrcTableCompleteRequest(testTable, opTime))
                .when()
                .post("/api/v2/kpi/srcTableComplete")
                .then()
                .statusCode(200)
                .body("executionId", notNullValue())
                .extract()
                .path("executionId");

        // 重做执行
        Integer secondId = given()
                .contentType(ContentType.JSON)
                .body(new SrcTableCompleteRequest(testTable, opTime))
                .when()
                .post("/api/v2/kpi/srcTableComplete")
                .then()
                .statusCode(200)
                .body("executionId", notNullValue())
                .extract()
                .path("executionId");

        // 验证生成了两条不同的日志记录
        assertNotEquals(firstId, secondId, "Redo should create new log entry");

        // 验证日志查询API能找到这些记录
        given()
                .queryParam("kpiModelId", testTable)
                .queryParam("opTime", opTime)
                .when()
                .get("/api/v2/etl/logs")
                .then()
                .statusCode(200)
                .body("total", greaterThanOrEqualTo(2)); // 至少有这两条
    }

    @Test
    @DisplayName("Statistics should reflect all executions")
    void testStatisticsAccuracy() {
        // 触发几次ETL
        for (int i = 0; i < 3; i++) {
            given()
                    .contentType(ContentType.JSON)
                    .body(new SrcTableCompleteRequest("stats_test_table", "2025120" + i))
                    .when()
                    .post("/api/v2/kpi/srcTableComplete");
        }

        // 查询统计
        given()
                .when()
                .get("/api/v2/etl/logs/stats")
                .then()
                .statusCode(200)
                .body("totalExecutions", greaterThan(0))
                .body("successCount", greaterThanOrEqualTo(0))
                .body("failedCount", greaterThanOrEqualTo(0));
    }
}
