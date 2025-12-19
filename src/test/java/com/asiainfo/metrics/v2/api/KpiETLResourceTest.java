package com.asiainfo.metrics.v2.api;

import com.asiainfo.metrics.common.model.dto.ETLModel;
import io.quarkus.test.junit.QuarkusTest;
import io.restassured.http.ContentType;
import org.junit.jupiter.api.Test;

import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.*;

/**
 * KpiETLResource 集成测试
 * 测试ETL API的日志记录功能
 */
@QuarkusTest
class KpiETLResourceTest {

    @Test
    void testETLTriggerWithLogging() {
        ETLModel etlModel = new ETLModel("test_table", "20251201");

        given()
                .contentType(ContentType.JSON)
                .body(etlModel)
                .when()
                .post("/api/v2/kpi/srcTableComplete")
                .then()
                .statusCode(200)
                .body("status", notNullValue())
                .body("tableName", equalTo("test_table"))
                .body("opTime", equalTo("20251201"))
                .body("executionId", notNullValue())
                .body("executionTimeMs", notNullValue());
    }
}
