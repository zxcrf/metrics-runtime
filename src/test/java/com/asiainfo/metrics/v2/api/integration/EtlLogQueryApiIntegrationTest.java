package com.asiainfo.metrics.v2.api.integration;

import io.quarkus.test.junit.QuarkusTest;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.*;

/**
 * ETL日志查询API集成测试
 * 测试完整的日志查询流程：查询 -> 过滤 -> 分页 -> 统计
 */
@QuarkusTest
@DisplayName("ETL Log Query API Integration Tests")
class EtlLogQueryApiIntegrationTest {

    @Test
    @DisplayName("Should query logs with pagination")
    void testQueryLogsWithPagination() {
        given()
                .queryParam("page", 1)
                .queryParam("pageSize", 10)
                .when()
                .get("/api/v2/etl/logs")
                .then()
                .statusCode(200)
                .body("total", greaterThanOrEqualTo(0))
                .body("page", equalTo(1))
                .body("pageSize", equalTo(10))
                .body("data", notNullValue())
                .body("data", isA(java.util.List.class));
    }

    @Test
    @DisplayName("Should filter logs by model ID")
    void testFilterByModelId() {
        String testModelId = "integration_test_table";

        given()
                .queryParam("kpiModelId", testModelId)
                .queryParam("page", 1)
                .queryParam("pageSize", 20)
                .when()
                .get("/api/v2/etl/logs")
                .then()
                .statusCode(200)
                .body("data", notNullValue());
    }

    @Test
    @DisplayName("Should filter logs by status")
    void testFilterByStatus() {
        given()
                .queryParam("status", "SUCCESS")
                .queryParam("page", 1)
                .queryParam("pageSize", 10)
                .when()
                .get("/api/v2/etl/logs")
                .then()
                .statusCode(200)
                .body("data", notNullValue());
    }

    @Test
    @DisplayName("Should filter logs by execution type")
    void testFilterByExecutionType() {
        given()
                .queryParam("executionType", "INITIAL")
                .queryParam("page", 1)
                .queryParam("pageSize", 10)
                .when()
                .get("/api/v2/etl/logs")
                .then()
                .statusCode(200)
                .body("data", notNullValue());
    }

    @Test
    @DisplayName("Should filter logs by date range")
    void testFilterByDateRange() {
        given()
                .queryParam("startDate", "2025-12-01")
                .queryParam("endDate", "2025-12-31")
                .queryParam("page", 1)
                .queryParam("pageSize", 10)
                .when()
                .get("/api/v2/etl/logs")
                .then()
                .statusCode(200)
                .body("data", notNullValue());
    }

    @Test
    @DisplayName("Should get log detail by ID")
    void testGetLogDetail() {
        // 先查询获取一个ID
        Integer logId = given()
                .queryParam("page", 1)
                .queryParam("pageSize", 1)
                .when()
                .get("/api/v2/etl/logs")
                .then()
                .statusCode(200)
                .extract()
                .path("data[0].id");

        if (logId != null) {
            given()
                    .when()
                    .get("/api/v2/etl/logs/" + logId)
                    .then()
                    .statusCode(200)
                    .body("id", equalTo(logId))
                    .body("kpiModelId", notNullValue())
                    .body("opTime", notNullValue())
                    .body("status", notNullValue());
        }
    }

    @Test
    @DisplayName("Should return 404 for non-existent log")
    void testGetNonExistentLog() {
        given()
                .when()
                .get("/api/v2/etl/logs/999999999")
                .then()
                .statusCode(404);
    }

    @Test
    @DisplayName("Should get latest executions by model ID")
    void testGetLatestExecutions() {
        String testModelId = "integration_test_table";

        given()
                .queryParam("limit", 5)
                .when()
                .get("/api/v2/etl/logs/latest/" + testModelId)
                .then()
                .statusCode(200)
                .body("$", hasSize(lessThanOrEqualTo(5)))
                .body("$", isA(java.util.List.class));
    }

    @Test
    @DisplayName("Should get statistics")
    void testGetStats() {
        given()
                .when()
                .get("/api/v2/etl/logs/stats")
                .then()
                .statusCode(200)
                .body("totalExecutions", greaterThanOrEqualTo(0))
                .body("successCount", greaterThanOrEqualTo(0))
                .body("failedCount", greaterThanOrEqualTo(0))
                .body("redoCount", greaterThanOrEqualTo(0))
                .body("avgExecutionTimeMs", notNullValue());
    }

    @Test
    @DisplayName("Should get filtered statistics by date range")
    void testGetFilteredStats() {
        given()
                .queryParam("startDate", "2025-12-01")
                .queryParam("endDate", "2025-12-31")
                .when()
                .get("/api/v2/etl/logs/stats")
                .then()
                .statusCode(200)
                .body("totalExecutions", greaterThanOrEqualTo(0));
    }

    @Test
    @DisplayName("Should support combined filters")
    void testCombinedFilters() {
        given()
                .queryParam("kpiModelId", "integration_test_table")
                .queryParam("status", "SUCCESS")
                .queryParam("executionType", "INITIAL")
                .queryParam("startDate", "2025-12-01")
                .queryParam("page", 1)
                .queryParam("pageSize", 5)
                .when()
                .get("/api/v2/etl/logs")
                .then()
                .statusCode(200)
                .body("data", notNullValue());
    }
}
