package com.asiainfo.metrics.v2;

import com.asiainfo.metrics.common.infrastructure.minio.KpiComputeService;
import com.asiainfo.metrics.common.infrastructure.minio.ParquetFileManager;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * DuckDB V2引擎业务验证测试
 * 包括：
 * 1. 派生指标 (KD9999 - 已存在)
 * 2. 四则运算计算指标 (KC8001, KC8002, KC8003, KC8004)
 * 3. 累加计算指标 (KC8005)
 * 4. 虚拟表达式指标 (无元数据)
 * 
 * 指标ID规则: K[DCYM]\d{4}
 * - 物理指标: KD1001, KD1002, KD1003, KD1006
 * - 复合指标: KC8001, KC8002, KC8003, KC8004, KC8005
 * - 派生指标: KD9999
 */
@QuarkusTest
public class DuckDBMetricValidationTest {

    private static final Logger log = LoggerFactory.getLogger(DuckDBMetricValidationTest.class);

    @Inject
    ParquetFileManager parquetFileManager;

    @Inject
    @io.quarkus.agroal.DataSource("metadb")
    io.agroal.api.AgroalDataSource metadbDataSource;

    /**
     * 主测试方法：生成所有测试数据
     */
    @Test
    public void generateAllMetricTestData() throws Exception {
        log.info("========================================");
        log.info("开始生成 DuckDB V2 引擎业务验证测试数据");
        log.info("========================================");

        // Step 1: 生成元数据到MetaDB
        insertMetadata();

        // Step 2: 生成维度表数据
        generateDimensionData();

        // Step 3: 生成物理指标数据（作为派生指标的基础）
        generatePhysicalMetricData();

        // Step 4: 验证数据生成结果
        log.info("========================================");
        log.info("所有测试数据生成完成！");
        log.info("========================================");
    }

    /**
     * 插入指标元数据到MetaDB (MySQL)
     */
    private void insertMetadata() throws Exception {
        log.info("开始插入指标元数据到MetaDB...");

        try (Connection conn = metadbDataSource.getConnection()) {

            // 清理旧数据
            cleanupOldMetadata(conn);

            // 1. 插入物理指标元数据
            insertPhysicalMetrics(conn);

            // 2. 插入派生指标元数据 (KD9999 已存在，这里确认)
            insertDerivedMetrics(conn);

            // 3. 插入四则运算指标元数据
            insertArithmeticMetrics(conn);

            // 4. 插入累加计算指标元数据
            insertCumulativeMetrics(conn);

            // MySQL 连接默认 autocommit=true，不需要手动 commit
            log.info("元数据插入完成");
        }
    }

    /**
     * 清理旧的测试元数据
     */
    private void cleanupOldMetadata(Connection conn) throws Exception {
        String sql = "DELETE FROM metrics_def WHERE kpi_id IN (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
        try (PreparedStatement stmt = conn.prepareStatement(sql)) {
            // 物理指标
            stmt.setString(1, "KD1001");
            stmt.setString(2, "KD1002");
            stmt.setString(3, "KD1003");
            stmt.setString(4, "KD1006");

            // 复合/计算指标
            stmt.setString(5, "KC8001"); // 加法
            stmt.setString(6, "KC8002"); // 减法
            stmt.setString(7, "KC8003"); // 乘法
            stmt.setString(8, "KC8004"); // 除法
            stmt.setString(9, "KC8005"); // 累加

            // 派生指标
            stmt.setString(10, "KD9999");

            stmt.executeUpdate();
            log.info("清理旧元数据完成");
        }
    }

    /**
     * 插入物理指标元数据
     */
    private void insertPhysicalMetrics(Connection conn) throws Exception {
        String sql = """
                    INSERT INTO metrics_def (
                        kpi_id, kpi_name, kpi_type, kpi_expr, agg_func,
                        comp_dim_code, model_id, t_state, team_name,
                        create_time, update_time
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, NOW(), NOW())
                """;

        // KD1001 - 物理指标1 (收入)
        executeInsert(conn, sql,
                "KD1001", "收入指标", "PHYSICAL", "${KD1001.current}",
                "sum", "CD002", "M001", "ACTIVE", "test_team");

        // KD1002 - 物理指标2 (成本)
        executeInsert(conn, sql,
                "KD1002", "成本指标", "PHYSICAL", "${KD1002.current}",
                "sum", "CD002", "M001", "ACTIVE", "test_team");

        // KD1003 - 物理指标3 (数量)
        executeInsert(conn, sql,
                "KD1003", "数量指标", "PHYSICAL", "${KD1003.current}",
                "sum", "CD002", "M001", "ACTIVE", "test_team");

        // KD1006 - 物理指标 (累加基础)
        executeInsert(conn, sql,
                "KD1006", "累加基础指标", "PHYSICAL", "${KD1006.current}",
                "sum", "CD002", "M001", "ACTIVE", "test_team");

        log.info("物理指标元数据插入完成");
    }

    /**
     * 插入派生指标元数据 (确认KD9999)
     */
    private void insertDerivedMetrics(Connection conn) throws Exception {
        String sql = """
                    INSERT INTO metrics_def (
                        kpi_id, kpi_name, kpi_type, kpi_expr, agg_func,
                        comp_dim_code, model_id, t_state, team_name,
                        create_time, update_time
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, NOW(), NOW())
                    ON DUPLICATE KEY UPDATE
                        kpi_name = VALUES(kpi_name),
                        kpi_type = VALUES(kpi_type),
                        kpi_expr = VALUES(kpi_expr),
                        update_time = NOW()
                """;

        // KD9999 - 派生指标（如果不存在则插入，存在则更新）
        executeInsert(conn, sql,
                "KD9999", "测试派生指标", "COMPOSITE", "${KD1001.current} + ${KD1002.current}",
                "sum", "CD002", "M001", "ACTIVE", "test_team");

        log.info("派生指标元数据确认完成");
    }

    /**
     * 插入四则运算指标元数据
     */
    private void insertArithmeticMetrics(Connection conn) throws Exception {
        String sql = """
                    INSERT INTO metrics_def (
                        kpi_id, kpi_name, kpi_type, kpi_expr, agg_func,
                        comp_dim_code, model_id, t_state, team_name,
                        create_time, update_time
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, NOW(), NOW())
                """;

        // KC8001 - 加法：收入 + 成本
        executeInsert(conn, sql,
                "KC8001", "加法指标-总额", "COMPOSITE", "${KD1001.current} + ${KD1002.current}",
                "sum", "CD002", "M001", "ACTIVE", "test_team");

        // KC8002 - 减法：收入 - 成本
        executeInsert(conn, sql,
                "KC8002", "减法指标-利润", "COMPOSITE", "${KD1001.current} - ${KD1002.current}",
                "sum", "CD002", "M001", "ACTIVE", "test_team");

        // KC8003 - 乘法：数量 * 收入
        executeInsert(conn, sql,
                "KC8003", "乘法指标-总价", "COMPOSITE", "${KD1003.current} * ${KD1001.current}",
                "sum", "CD002", "M001", "ACTIVE", "test_team");

        // KC8004 - 除法：收入 / 成本
        executeInsert(conn, sql,
                "KC8004", "除法指标-投入产出比", "COMPOSITE", "${KD1001.current} / ${KD1002.current}",
                "sum", "CD002", "M001", "ACTIVE", "test_team");

        log.info("四则运算指标元数据插入完成");
    }

    /**
     * 插入累加计算指标元数据
     */
    private void insertCumulativeMetrics(Connection conn) throws Exception {
        String sql = """
                    INSERT INTO metrics_def (
                        kpi_id, kpi_name, kpi_type, kpi_expr, agg_func,
                        comp_dim_code, model_id, t_state, team_name,
                        create_time, update_time
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, NOW(), NOW())
                """;

        // KC8005 - 累加：当期值 + 上期值
        executeInsert(conn, sql,
                "KC8005", "累加指标-累计值", "COMPOSITE", "${KD1006.current} + ${KD1006.lastCycle}",
                "sum", "CD002", "M001", "ACTIVE", "test_team");

        log.info("累加指标元数据插入完成");
    }

    /**
     * 执行INSERT语句的辅助方法
     */
    private void executeInsert(Connection conn, String sql, String... params) throws Exception {
        try (PreparedStatement stmt = conn.prepareStatement(sql)) {
            for (int i = 0; i < params.length; i++) {
                stmt.setString(i + 1, params[i]);
            }
            stmt.executeUpdate();
        }
    }

    /**
     * 生成维度表数据
     */
    private void generateDimensionData() throws Exception {
        log.info("开始生成维度表数据...");

        String compDimCode = "CD002";
        List<Map<String, String>> dimRecords = new ArrayList<>();

        // 生成 50 个城市维度和对应的区县
        for (int i = 1; i <= 50; i++) {
            String cityCode = String.format("C%04d", i);

            // City 行
            Map<String, String> cityRow = new HashMap<>();
            cityRow.put("dim_code", cityCode);
            cityRow.put("dim_val", "City_" + i);
            cityRow.put("dim_id", "city_id");
            cityRow.put("parent_dim_code", null);
            dimRecords.add(cityRow);

            // County 行
            Map<String, String> countyRow = new HashMap<>();
            String countyCode = "K" + cityCode;
            countyRow.put("dim_code", countyCode);
            countyRow.put("dim_val", "County_Of_" + i);
            countyRow.put("dim_id", "county_id");
            countyRow.put("parent_dim_code", cityCode);
            dimRecords.add(countyRow);
        }

        // 创建并上传维度表 Parquet 文件
        String dimLocalPath = parquetFileManager.createDimParquetFilePath(compDimCode);
        String dimTableName = "kpi_dim_" + compDimCode;

        parquetFileManager.writeDimDataToParquet(dimLocalPath, dimTableName, dimRecords);
        parquetFileManager.uploadDimParquetFile(dimLocalPath, compDimCode);

        // 清理本地文件
        java.nio.file.Files.deleteIfExists(java.nio.file.Paths.get(dimLocalPath));
        log.info("维度表数据生成完成: {}", compDimCode);
    }

    /**
     * 生成物理指标数据
     */
    private void generatePhysicalMetricData() throws Exception {
        log.info("开始生成物理指标数据...");

        String compDimCode = "CD002";

        // 生成15个时间点的数据（20251101-20251115）
        for (int day = 1; day <= 15; day++) {
            String opTime = String.format("202511%02d", day);

            // 生成各个物理指标的数据
            generateMetricDataForOneTimepoint("KD1001", opTime, compDimCode, 50, 1000.0, 5000.0);
            generateMetricDataForOneTimepoint("KD1002", opTime, compDimCode, 50, 500.0, 2000.0);
            generateMetricDataForOneTimepoint("KD1003", opTime, compDimCode, 50, 10.0, 100.0);
            generateMetricDataForOneTimepoint("KD1006", opTime, compDimCode, 50, 100.0, 1000.0);
        }

        log.info("物理指标数据生成完成");
    }

    /**
     * 为单个指标生成一个时间点的数据
     */
    private void generateMetricDataForOneTimepoint(
            String kpiId, String opTime, String compDimCode,
            int count, double minValue, double maxValue) throws Exception {

        log.info("生成指标数据: kpiId={}, opTime={}", kpiId, opTime);

        List<KpiComputeService.KpiDataRecord> records = new ArrayList<>();

        for (int i = 1; i <= count; i++) {
            Map<String, Object> dimValues = new HashMap<>();

            String cityCode = String.format("C%04d", i);
            String countyCode = "K" + cityCode;

            dimValues.put("city_id", cityCode);
            dimValues.put("county_id", countyCode);
            dimValues.put("op_time", opTime);

            // 在指定范围内生成随机值
            double kpiVal = minValue + Math.random() * (maxValue - minValue);

            KpiComputeService.KpiDataRecord record = new KpiComputeService.KpiDataRecord(
                    kpiId, opTime, compDimCode, dimValues, kpiVal);

            records.add(record);
        }

        // 创建Parquet文件
        String localPath = parquetFileManager.createParquetFilePath(kpiId, opTime, compDimCode);
        String tableName = parquetFileManager.getParquetTableName(kpiId, opTime, compDimCode);

        // 写入Parquet
        parquetFileManager.writeDataToParquet(localPath, tableName, records);

        // 上传到MinIO
        parquetFileManager.uploadParquetFile(localPath, kpiId, opTime, compDimCode);

        // 清理本地文件
        java.nio.file.Files.deleteIfExists(java.nio.file.Paths.get(localPath));

        log.info("完成: kpiId={}, opTime={}", kpiId, opTime);
    }

    /**
     * 测试查询派生指标（KD9999）
     */
    @Test
    public void testQueryDerivedMetric() {
        log.info("测试查询派生指标 KD9999");
        // TODO: 调用实际的查询API验证
    }

    /**
     * 测试查询四则运算指标
     */
    @Test
    public void testQueryArithmeticMetrics() {
        log.info("测试查询四则运算指标");
        // TODO: 分别测试 KC8001, KC8002, KC8003, KC8004
    }

    /**
     * 测试查询累加计算指标
     */
    @Test
    public void testQueryCumulativeMetric() {
        log.info("测试查询累加指标 KC8005");
        // TODO: 调用实际的查询API验证
    }

    /**
     * 测试虚拟表达式指标（无元数据）
     */
    @Test
    public void testVirtualExpressionMetric() {
        log.info("测试虚拟表达式指标");
        // TODO: 直接传入表达式，无需元数据
        // 示例：传入表达式 "${KD1001.current} * 1.1" 进行计算
    }
}
