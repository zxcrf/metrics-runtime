package com.asiainfo.metrics.v2.api;

import io.quarkus.test.junit.QuarkusTest;
import org.junit.jupiter.api.Test;

import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.*;

/**
 * ETLLogQueryResource 集成测试
 */
@QuarkusTest
class ETLLogQueryResourceTest {

    @Test
    void testQueryLogs() {
        given()
                .queryParam("page", 1)
                .queryParam("pageSize", 10)
                .when()
                .get("/api/v2/etl/logs")
                .then()
                .statusCode(200)
                .body("total", notNullValue())
                .body("page", equalTo(1))
                .body("pageSize", equalTo(10))
                .body("data", notNullValue());
    }

    @Test
    void testQueryLogsByModel() {
        given()
                .queryParam("kpiModelId", "test_table")
                .queryParam("page", 1)
                .queryParam("pageSize", 10)
                .when()
                .get("/api/v2/etl/logs")
                .then()
                .statusCode(200)
                .body("data", notNullValue());
    }

    @Test
    void testGetStats() {
        given()
                .when()
                .get("/api/v2/etl/logs/stats")
                .then()
                .statusCode(200)
                .body("totalExecutions", notNullValue())
                .body("successCount", notNullValue())
                .body("failedCount", notNullValue());
    }

    @Test
    void testGetLatestExecutions() {
        given()
                .queryParam("limit", 5)
                .when()
                .get("/api/v2/etl/logs/latest/test_table")
                .then()
                .statusCode(200)
                .body("$", hasSize(lessThanOrEqualTo(5)));
    }

    @Test
    void testGetLogDetailNotFound() {
        given()
                .when()
                .get("/api/v2/etl/logs/999999")
                .then()
                .statusCode(404);
    }
}
