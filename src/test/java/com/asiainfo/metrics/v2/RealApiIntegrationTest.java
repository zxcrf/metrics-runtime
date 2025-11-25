package com.asiainfo.metrics.v2;

// ... imports 保持不变 ...
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

    private String dbPath1Str;
    private String dbPath2Str;
    private String dimDbPath1Str; // CD001 维度表
    private String dimDbPath2Str; // CD002 维度表

    @BeforeEach
    public void setup() throws Exception {
        String opTime = "20251104";

        // 1. Mock 元数据
        Mockito.when(metadataRepo.findById("KD1001")).thenReturn(MetricDefinition.physical("KD1001", "sum", "CD001"));
        Mockito.when(metadataRepo.getDimCols("CD001")).thenReturn(Set.of("city_id"));

        Mockito.when(metadataRepo.findById("KD1002")).thenReturn(MetricDefinition.physical("KD1002", "sum", "CD002"));
        Mockito.when(metadataRepo.getDimCols("CD002")).thenReturn(Set.of("city_id", "county_id"));

        // 2. 准备物理文件
        Path tempDir = Files.createTempDirectory("metrics_test_");
        tempDir.toFile().deleteOnExit();

        // KPI 表 1
        Path dbPath1 = tempDir.resolve("kd1001.db");
        this.dbPath1Str = dbPath1.toAbsolutePath().toString();
        try (Connection conn = DriverManager.getConnection("jdbc:sqlite:" + dbPath1Str)) {
            Statement stmt = conn.createStatement();
            String tableName = "kpi_KD1001_" + opTime + "_CD001";
            stmt.execute("CREATE TABLE " + tableName + " (city_id TEXT, kpi_val REAL, kpi_id TEXT, op_time TEXT)");
            stmt.execute("INSERT INTO " + tableName + " VALUES ('999', 100.0, 'KD1001', '" + opTime + "')");
        }

        // KPI 表 2
        Path dbPath2 = tempDir.resolve("kd1002.db");
        this.dbPath2Str = dbPath2.toAbsolutePath().toString();
        try (Connection conn = DriverManager.getConnection("jdbc:sqlite:" + dbPath2Str)) {
            Statement stmt = conn.createStatement();
            String tableName = "kpi_KD1002_" + opTime + "_CD002";
            stmt.execute("CREATE TABLE " + tableName + " (city_id TEXT, county_id TEXT, kpi_val REAL, kpi_id TEXT, op_time TEXT)");
            stmt.execute("INSERT INTO " + tableName + " VALUES ('999', '100', 200.0, 'KD1002', '" + opTime + "')");
        }

        // 维度表 1 (CD001)
        Path dimDbPath1 = tempDir.resolve("dim_cd001.db");
        this.dimDbPath1Str = dimDbPath1.toAbsolutePath().toString();
        try (Connection conn = DriverManager.getConnection("jdbc:sqlite:" + dimDbPath1Str)) {
            Statement stmt = conn.createStatement();
            // 注意：表名通常是 kpi_dim_{CODE}
            stmt.execute("CREATE TABLE kpi_dim_CD001 (dim_code TEXT, dim_val TEXT, city_id TEXT, city_id_desc TEXT)");
            stmt.execute("INSERT INTO kpi_dim_CD001 VALUES ('999', 'Beijing', '999', 'Beijing')");
        }

        // 维度表 2 (CD002)
        Path dimDbPath2 = tempDir.resolve("dim_cd002.db");
        this.dimDbPath2Str = dimDbPath2.toAbsolutePath().toString();
        try (Connection conn = DriverManager.getConnection("jdbc:sqlite:" + dimDbPath2Str)) {
            Statement stmt = conn.createStatement();
            stmt.execute("CREATE TABLE kpi_dim_CD002 (dim_code TEXT, dim_val TEXT, city_id TEXT, city_id_desc TEXT, county_id TEXT, county_id_desc TEXT)");
            stmt.execute("INSERT INTO kpi_dim_CD002 VALUES ('999|100', 'Beijing-Chaoyang', '999', 'Beijing', '100', 'Chaoyang')");
        }

        // 3. Mock StorageManager
        // Mock KPI 下载
        Mockito.when(storageManager.downloadAndPrepare(ArgumentMatchers.any(PhysicalTableReq.class)))
                .thenAnswer(invocation -> {
                    PhysicalTableReq req = invocation.getArgument(0);
                    if ("KD1001".equals(req.kpiId())) return dbPath1Str;
                    if ("KD1002".equals(req.kpiId())) return dbPath2Str;
                    return dbPath1Str;
                });

        // Mock 维度表下载 (关键修复)
        Mockito.when(storageManager.downloadAndCacheDimDB(ArgumentMatchers.anyString()))
                .thenAnswer(invocation -> {
                    String code = invocation.getArgument(0);
                    if ("CD001".equals(code)) return dimDbPath1Str;
                    if ("CD002".equals(code)) return dimDbPath2Str;
                    return dimDbPath1Str;
                });
    }

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
                .post("/api/v2/kpi/queryKpiData") // 确保路径正确
                .then()
                .statusCode(200)
                .body("status", equalTo("0000"))
                .body("dataArray", hasSize(2));
    }

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
                .post("/api/v2/kpi/queryKpiData")
                .then()
                .statusCode(200)
                .body("status", equalTo("0000"))
                .body("dataArray", hasSize(1))
                .body("dataArray[0].toString()", containsString("300.0"));
    }
}