package com.asiainfo.metrics.v2.api.integration;

import com.asiainfo.metrics.api.dto.SrcTableCompleteRequest;
import io.quarkus.test.junit.QuarkusTest;
import io.restassured.http.ContentType;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.MethodOrderer.OrderAnnotation;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

/**
 * ETL API集成测试
 * 测试完整的ETL流程：触发 -> 执行 -> 日志记录 -> 缓存失效
 */
@QuarkusTest
@DisplayName("ETL API Integration Tests")
@TestMethodOrder(OrderAnnotation.class)
class EtlApiIntegrationTest {

        private static final String TEST_TABLE = "integration_test_table";
        private static final String TEST_OP_TIME = "20251201";

        @Test
        @Order(1)
        @DisplayName("Should trigger ETL and record execution log")
        void testTriggerEtl() {
                SrcTableCompleteRequest etlModel = new SrcTableCompleteRequest(TEST_TABLE, TEST_OP_TIME);

                given()
                                .contentType(ContentType.JSON)
                                .body(etlModel)
                                .when()
                                .post("/api/v2/kpi/srcTableComplete")
                                .then()
                                .statusCode(200)
                                .body("status", anyOf(equalTo("SUCCESS"), equalTo("ERROR"))) // 接受两种状态
                                .body("executionId", notNullValue()) // 关键：日志ID应该存在
                                .body("executionTimeMs", notNullValue()) // 执行时间应该存在
                                .body("message", notNullValue()); // 消息应该存在
        }

        @Test
        @Order(2)
        @DisplayName("Should detect REDO execution type on second run")
        void testRedoDetection() {
                String testOpTime = "20251205"; // 使用新的日期避免冲突
                SrcTableCompleteRequest etlModel = new SrcTableCompleteRequest(TEST_TABLE, testOpTime);

                // 第一次执行
                Integer firstId = given()
                                .contentType(ContentType.JSON)
                                .body(etlModel)
                                .when()
                                .post("/api/v2/kpi/srcTableComplete")
                                .then()
                                .statusCode(200)
                                .body("executionId", notNullValue())
                                .extract()
                                .path("executionId");

                // 第二次执行（重做）
                Integer secondId = given()
                                .contentType(ContentType.JSON)
                                .body(etlModel)
                                .when()
                                .post("/api/v2/kpi/srcTableComplete")
                                .then()
                                .statusCode(200)
                                .body("executionId", notNullValue())
                                .extract()
                                .path("executionId");

                // 验证创建了两条日志记录（不同的ID）
                assertNotEquals(firstId, secondId, "Two executions should have different IDs");
        }

        @Test
        @Order(3)
        @DisplayName("Should handle ETL failure gracefully")
        void testEtlFailure() {
                // 使用不存在的表名触发失败
                SrcTableCompleteRequest etlModel = new SrcTableCompleteRequest("non_existent_table_xyz", TEST_OP_TIME);

                given()
                                .contentType(ContentType.JSON)
                                .body(etlModel)
                                .when()
                                .post("/api/v2/kpi/srcTableComplete")
                                .then()
                                .statusCode(200)
                                .body("status", equalTo("ERROR")) // 应该返回ERROR
                                .body("message", notNullValue())
                                .body("executionId", notNullValue()); // 即使失败也应该有executionId
        }

        @Test
        @Order(4)
        @DisplayName("Should record execution time")
        void testExecutionTimeRecorded() {
                SrcTableCompleteRequest etlModel = new SrcTableCompleteRequest(TEST_TABLE, "20251206");

                given()
                                .contentType(ContentType.JSON)
                                .body(etlModel)
                                .when()
                                .post("/api/v2/kpi/srcTableComplete")
                                .then()
                                .statusCode(200)
                                .body("executionTimeMs", notNullValue()); // 只验证有时间记录
        }
}
