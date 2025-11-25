package com.asiainfo.metrics.v2;

import com.asiainfo.metrics.v2.core.model.MetricDefinition;
import com.asiainfo.metrics.v2.core.model.PhysicalTableReq;
import com.asiainfo.metrics.v2.infra.persistence.MetadataRepository;
import com.asiainfo.metrics.v2.infra.storage.StorageManager;
import io.quarkus.test.InjectMock;
import io.quarkus.test.junit.QuarkusTest;
import io.restassured.RestAssured;
import io.restassured.http.ContentType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.Set;

import static org.hamcrest.Matchers.*;

@QuarkusTest
public class RealApiIntegrationTest {

    @InjectMock
    MetadataRepository metadataRepo;

    @InjectMock
    StorageManager storageManager;

    // 保存临时数据库路径，供Mock回调使用
    private String dbPath1Str;
    private String dbPath2Str;

    @BeforeEach
    public void setup() throws Exception {
        // 0. 防御性检查：确保Mock注入成功
        if (metadataRepo == null || storageManager == null) {
            throw new IllegalStateException("Mock injection failed! metadataRepo=" + metadataRepo + ", storageManager=" + storageManager);
        }

        String opTime = "20251104";

        // ---------------------------------------------------------
        // 1. 准备 Mock 元数据
        // ---------------------------------------------------------
        // KD1001: 属于 CD001 (只有 city_id)
        Mockito.when(metadataRepo.findById("KD1001"))
                .thenReturn(MetricDefinition.physical("KD1001", "sum", "CD001"));
        Mockito.when(metadataRepo.getDimCols("CD001")).thenReturn(Set.of("city_id"));

        // KD1002: 属于 CD002 (有 city_id, county_id)
        Mockito.when(metadataRepo.findById("KD1002"))
                .thenReturn(MetricDefinition.physical("KD1002", "sum", "CD002"));
        Mockito.when(metadataRepo.getDimCols("CD002")).thenReturn(Set.of("city_id", "county_id"));

        // ---------------------------------------------------------
        // 2. 准备真实的 SQLite 文件 (手动创建临时目录，不依赖 @TempDir)
        // ---------------------------------------------------------
        Path tempDir = Files.createTempDirectory("metrics_test_");
        tempDir.toFile().deleteOnExit(); // 测试结束后自动清理

        // === 准备 KD1001 数据 (粒度: 市) ===
        Path dbPath1 = tempDir.resolve("kd1001.db");
        this.dbPath1Str = dbPath1.toAbsolutePath().toString();

        try (Connection conn = DriverManager.getConnection("jdbc:sqlite:" + dbPath1Str)) {
            Statement stmt = conn.createStatement();
            // 表名必须匹配: kpi_{id}_{time}_{dim}
            String tableName = "kpi_KD1001_" + opTime + "_CD001";
            stmt.execute("CREATE TABLE " + tableName + " (city_id TEXT, kpi_val REAL, kpi_id TEXT, op_time TEXT)");
            // 插入数据: 北京(999), 值 100
            stmt.execute("INSERT INTO " + tableName + " VALUES ('999', 100.0, 'KD1001', '" + opTime + "')");
        }

        // === 准备 KD1002 数据 (粒度: 市 + 区县) ===
        Path dbPath2 = tempDir.resolve("kd1002.db");
        this.dbPath2Str = dbPath2.toAbsolutePath().toString();

        try (Connection conn = DriverManager.getConnection("jdbc:sqlite:" + dbPath2Str)) {
            Statement stmt = conn.createStatement();
            String tableName = "kpi_KD1002_" + opTime + "_CD002";
            stmt.execute("CREATE TABLE " + tableName + " (city_id TEXT, county_id TEXT, kpi_val REAL, kpi_id TEXT, op_time TEXT)");
            // 插入数据: 北京(999), 朝阳(100), 值 200
            stmt.execute("INSERT INTO " + tableName + " VALUES ('999', '100', 200.0, 'KD1002', '" + opTime + "')");
        }

        // ---------------------------------------------------------
        // 3. Mock StorageManager (拦截下载请求，指向刚才创建的临时文件)
        // ---------------------------------------------------------
        Mockito.when(storageManager.downloadAndPrepare(ArgumentMatchers.any(PhysicalTableReq.class)))
                .thenAnswer(invocation -> {
                    PhysicalTableReq req = invocation.getArgument(0);
                    if ("KD1001".equals(req.kpiId())) return dbPath1Str;
                    if ("KD1002".equals(req.kpiId())) return dbPath2Str;
                    // 兜底返回，防止NPE
                    return dbPath1Str;
                });
    }

    /**
     * 场景：混合维度查询 [city_id, county_id]
     * KD1001 缺 county_id -> 自动补 NULL
     * KD1002 全都有 -> 正常查询
     */
    @Test
    public void testMixedDimensionQuery() {
        String requestJson = """
            {
                "kpiArray": ["KD1001", "KD1002"],
                "opTimeArray": ["20251104"],
                "dimCodeArray": ["city_id", "county_id"],
                "includeHistoricalData": false,
                "includeTargetData": false
            }
        """;

        RestAssured.given()
                .contentType(ContentType.JSON)
                .body(requestJson)
                .when()
                .post("/api/v2/kpi/queryKpiData") // 修正了 URL 路径
                .then()
                .statusCode(200)
                .body("status", equalTo("0000"))
                .body("dataArray", hasSize(2))
                // 验证数据: KD1001 行，county_id 应该不存在或为 null
                .body("dataArray.find { it.KD1001 != '--' }.KD1001", equalTo("100.0"))
                // 验证数据: KD1002 行，county_id 应该为 '100'
                .body("dataArray.find { it.KD1002 != '--' }.KD1002", equalTo("200.0"));
    }

    /**
     * 场景：虚拟指标聚合查询
     * V1 = ${KD1001} + ${KD1002}
     * 按 city_id 聚合 (100 + 200 = 300)
     */
    @Test
    public void testVirtualKpiAggregated() {
        String requestJson = """
            {
                "kpiArray": ["${KD1001} + ${KD1002}"],
                "opTimeArray": ["20251104"],
                "dimCodeArray": ["city_id"], 
                "includeHistoricalData": false,
                "includeTargetData": false
            }
        """;

        RestAssured.given()
                .contentType(ContentType.JSON)
                .body(requestJson)
                .when()
                .post("/api/v2/kpi/queryKpiData") // 修正了 URL 路径
                .then()
                .statusCode(200)
                .body("status", equalTo("0000"))
                .body("dataArray", hasSize(1))
                .body("dataArray[0].city_id", equalTo("999"))
                // 验证计算结果: 应该包含 300.0
                .body("dataArray[0].toString()", containsString("300.0"));
    }
}