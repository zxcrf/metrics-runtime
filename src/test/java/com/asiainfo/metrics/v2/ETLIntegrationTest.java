package com.asiainfo.metrics.v2;

import com.asiainfo.metrics.common.model.dto.ETLModel;
import io.quarkus.test.junit.QuarkusTest;
import io.restassured.http.ContentType;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;

/**
 * V2 ETL API 集成测试
 * 测试 /api/v2/kpi/srcTableComplete 接口
 *
 * @author QvQ
 * @date 2025/11/28
 */
@QuarkusTest
public class ETLIntegrationTest {

    private static final Logger log = LoggerFactory.getLogger(ETLIntegrationTest.class);

    /**
     * 测试 V2 ETL API 基本功能
     * 前置条件：需要确保数据库中有对应的元数据和源表数据
     */
    @Test
    public void testV2ETLEndpoint() {
        log.info("========================================");
        log.info("测试 V2 ETL API");
        log.info("========================================");

        // 构造 ETL 请求
        ETLModel etlModel = new ETLModel(
                "rpt_khgj_result_user_dd_yyyymmdd", // 源表名称（需要在元数据中配置）
                "20251024" // 批次时间
        );

        // 调用 V2 ETL API
        given()
                .contentType(ContentType.JSON)
                .body(etlModel)
                .when()
                .post("/api/v2/kpi/srcTableComplete")
                .then()
                .statusCode(200)
                .body("status", notNullValue())
                .body("message", notNullValue())
                .log().all();

        log.info("V2 ETL API 测试完成");
    }

    /**
     * 测试 V2 ETL API 错误处理
     * 使用不存在的表名，应该返回错误
     */
    @Test
    public void testV2ETLEndpoint_InvalidTable() {
        log.info("========================================");
        log.info("测试 V2 ETL API - 错误处理");
        log.info("========================================");

        // 构造无效的 ETL 请求
        ETLModel etlModel = new ETLModel(
                "non_existent_table", // 不存在的表
                "20251128");

        // 调用 V2 ETL API，预期返回错误
        given()
                .contentType(ContentType.JSON)
                .body(etlModel)
                .when()
                .post("/api/v2/kpi/srcTableComplete")
                .then()
                .statusCode(200)
                .body("status", equalTo("ERROR"))
                .body("message", notNullValue())
                .log().all();

        log.info("V2 ETL API 错误处理测试完成");
    }
}
