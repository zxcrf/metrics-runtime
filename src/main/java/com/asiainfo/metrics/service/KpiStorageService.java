package com.asiainfo.metrics.service;

import com.asiainfo.metrics.config.MetricsConfig;
import io.agroal.api.AgroalDataSource;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.*;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.zip.GZIPOutputStream;

/**
 * 指标存储服务
 * 支持MySQL和SQLite两种存储引擎
 *
 * 业务说明：
 * - 存储时只保存当前批次的kpi_val值
 * - last_cycle（环比）和last_year（同比）在查询时动态计算
 * - 这种设计符合从数据仓库同步指标数据的业务场景
 *
 * @author QvQ
 * @date 2025/11/12
 */
@ApplicationScoped
public class KpiStorageService {

    private static final Logger log = LoggerFactory.getLogger(KpiStorageService.class);

    @Inject
    @io.quarkus.agroal.DataSource("metadb")
    AgroalDataSource metadbDataSource;

    @Inject
    MetricsConfig metricsConfig;

    @Inject
    MinIOService minioService;

    /**
     * 存储指标数据到数据库
     *
     * @param records 指标数据记录列表
     * @param engineType 存储引擎类型：MYSQL 或 SQLITE
     * @return 存储结果
     */
    public StorageResult storageMetrics(List<KpiComputeService.KpiDataRecord> records, String engineType) {
        try {
            if (records == null || records.isEmpty()) {
                return StorageResult.error("没有数据需要存储");
            }

            log.info("开始存储 {} 条指标数据，使用引擎：{}", records.size(), engineType);

            // 引擎类型映射
            String normalizedEngine = engineType.toUpperCase();
            if (!normalizedEngine.equals("MYSQL") && !normalizedEngine.equals("SQLITE")) {
                return StorageResult.error("不支持的存储引擎类型：" + engineType);
            }

            // 根据引擎类型存储
            if (normalizedEngine.equals("MYSQL")) {
                return storageToMySQL(records);
            } else {
                return storageToSQLite(records);
            }

        } catch (Exception e) {
            log.error("存储指标数据失败", e);
            return StorageResult.error("存储失败: " + e.getMessage());
        }
    }

    /**
     * 存储到MySQL数据库
     */
    private StorageResult storageToMySQL(List<KpiComputeService.KpiDataRecord> records) throws SQLException {
        // 按数据表分组
        Map<String, List<KpiComputeService.KpiDataRecord>> groupedByTable = records.stream()
                .collect(java.util.stream.Collectors.groupingBy(record ->
                        "kpi_" + getCycleType(record.opTime()).toLowerCase() + "_" + record.compDimCode()));

        int totalStored = 0;
        StringBuilder resultMsg = new StringBuilder();

        for (Map.Entry<String, List<KpiComputeService.KpiDataRecord>> entry : groupedByTable.entrySet()) {
            String tableName = entry.getKey();
            List<KpiComputeService.KpiDataRecord> tableRecords = entry.getValue();

            log.info("存储数据到表：{}，记录数：{}", tableName, tableRecords.size());

            int stored = insertBatchToMySQL(tableName, tableRecords);
            totalStored += stored;

            if (resultMsg.length() > 0) {
                resultMsg.append("; ");
            }
            resultMsg.append(tableName).append(": ").append(stored);
        }

        log.info("MySQL存储完成，共存储 {} 条记录", totalStored);
        return StorageResult.success("MySQL存储成功: " + resultMsg, totalStored);
    }

    /**
     * 批量插入MySQL数据
     */
    private int insertBatchToMySQL(String tableName, List<KpiComputeService.KpiDataRecord> records) throws SQLException {
        if (records.isEmpty()) {
            return 0;
        }

        // 构建INSERT SQL
        String sql = buildInsertSqlForMySQL(tableName, records.get(0));
        log.info("MySQL插入SQL：{}", sql);

        try (Connection conn = metadbDataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {

            // 设置批处理数据
            for (KpiComputeService.KpiDataRecord record : records) {
                setStatementParameters(stmt, record, 1);
                stmt.addBatch();
            }

            // 执行批处理
            int[] results = stmt.executeBatch();
            int totalAffected = 0;
            for (int result : results) {
                if (result > 0) {
                    totalAffected += result;
                }
            }

            log.debug("MySQL插入完成，影响行数：{}", totalAffected);
            return totalAffected;
        }
    }

    /**
     * 构建MySQL INSERT SQL
     * 只存储当前值kpi_val，last_cycle和last_year在查询时计算
     */
    private String buildInsertSqlForMySQL(String tableName, KpiComputeService.KpiDataRecord record) {
        // 获取真正的维度字段名列表
        List<String> dimFieldNames = getDimFieldNames(record.compDimCode());

        StringBuilder columns = new StringBuilder("kpi_id, op_time, kpi_val");
        StringBuilder placeholders = new StringBuilder("?, ?, ?");

        // 只添加真正的维度字段
        for (String dimFieldName : dimFieldNames) {
            columns.append(", ").append(dimFieldName);
            placeholders.append(", ?");
        }

        // 注意：last_year_val 和 last_cycle_val 不在这里设置
        // 它们会在查询时通过时间点过滤动态计算

        return String.format("INSERT INTO %s (%s) VALUES (%s) ON DUPLICATE KEY UPDATE kpi_val = VALUES(kpi_val)",
                tableName, columns.toString(), placeholders.toString());
    }

    /**
     * 设置PreparedStatement参数
     */
    private void setStatementParameters(PreparedStatement stmt, KpiComputeService.KpiDataRecord record, int startIdx) throws SQLException {
        int idx = startIdx;

        stmt.setString(idx++, record.kpiId());
        stmt.setString(idx++, record.opTime());
        stmt.setObject(idx++, record.kpiVal());

        // 设置维度字段
        // 注意：不能直接遍历 dimValues，因为它可能包含派生指标编码等非维度字段
        // 需要根据 compDimCode 获取真正的维度字段列表
        List<String> dimFieldNames = getDimFieldNames(record.compDimCode());

        Map<String, Object> dimValues = record.dimValues();
        for (String dimFieldName : dimFieldNames) {
            Object dimValue = dimValues.get(dimFieldName);
            stmt.setObject(idx++, dimValue);
        }
    }

    /**
     * 根据组合维度编码获取维度字段名列表
     * 只返回真正的维度字段，不包含派生指标编码
     */
    private List<String> getDimFieldNames(String compDimCode) {
        // 这里需要调用 Repository 的方法获取维度定义
        // 但由于这是 StorageService，不能直接注入 Repository
        // 暂时硬编码映射（后续优化）
        return switch (compDimCode) {
            case "CD001" -> List.of("city_id");
            case "CD002" -> List.of("city_id", "county_id");
            case "CD003" -> List.of("city_id", "county_id", "region_id");
            default -> List.of("city_id", "county_id", "region_id");
        };
    }

    /**
     * 存储到SQLite文件
     * SQLite模式会生成独立的.db文件，后续需要上传到MinIO
     */
    private StorageResult storageToSQLite(List<KpiComputeService.KpiDataRecord> records) {
        try {
            if (records.isEmpty()) {
                return StorageResult.success("无数据需要存储", 0);
            }

            // 按KPI分组
            Map<String, List<KpiComputeService.KpiDataRecord>> kpiGroup = records.stream()
                .collect(Collectors.groupingBy(KpiComputeService.KpiDataRecord::kpiId));

            int totalStored = 0;

            // 为每个KPI创建单独的SQLite文件
            for (Map.Entry<String, List<KpiComputeService.KpiDataRecord>> entry : kpiGroup.entrySet()) {
                String kpiId = entry.getKey();
                List<KpiComputeService.KpiDataRecord> kpiRecords = entry.getValue();

                // 获取第一个记录的元数据（所有记录的kpiId, opTime, compDimCode应该相同）
                KpiComputeService.KpiDataRecord firstRecord = kpiRecords.get(0);
                String opTime = firstRecord.opTime();
                String compDimCode = firstRecord.compDimCode();

                // 构建文件路径
                String localPath = String.format("/tmp/cache/%s_%s_%s.db", kpiId, opTime, compDimCode);
                String tableName = String.format("kpi_%s_%s_%s", kpiId, opTime, compDimCode);

                log.info("创建SQLite文件: {}, 表名: {}", localPath, tableName);

                // 创建SQLite数据库文件
                try (Connection conn = DriverManager.getConnection("jdbc:sqlite:" + localPath)) {
                    // 建表
                    createSQLiteTable(conn, tableName, kpiRecords);

                    // 插入数据
                    insertSQLiteData(conn, tableName, kpiRecords);

                    totalStored += kpiRecords.size();
                }

                // 压缩并上传到MinIO
                String compressedPath = localPath + ".gz";
                try (FileInputStream fis = new FileInputStream(localPath);
                     GZIPOutputStream gzos = new GZIPOutputStream(
                         Files.newOutputStream(Paths.get(compressedPath)))) {

                    byte[] buffer = new byte[8192];
                    int bytesRead;
                    while ((bytesRead = fis.read(buffer)) != -1) {
                        gzos.write(buffer, 0, bytesRead);
                    }
                }

                // 上传到MinIO
                String s3Key = String.format("metrics/%s/%s/%s/%s_%s_%s.db.gz",
                    kpiId, opTime, compDimCode, kpiId, opTime, compDimCode);
                minioService.uploadResult(compressedPath, s3Key);

                log.info("SQLite文件已上传到MinIO: {}", s3Key);

                // 清理临时文件
                Files.deleteIfExists(Paths.get(localPath));
                Files.deleteIfExists(Paths.get(compressedPath));
            }

            log.info("SQLite存储完成，共存储 {} 条记录", totalStored);
            return StorageResult.success("SQLite文件生成并上传成功", totalStored);

        } catch (Exception e) {
            log.error("SQLite存储失败", e);
            return StorageResult.error("SQLite存储失败: " + e.getMessage());
        }
    }

    /**
     * 创建SQLite表结构
     */
    private void createSQLiteTable(Connection conn, String tableName,
                                   List<KpiComputeService.KpiDataRecord> records) throws SQLException {
        if (records.isEmpty()) {
            return;
        }

        // 获取维度字段名（排除op_time字段）
        KpiComputeService.KpiDataRecord firstRecord = records.get(0);
        Map<String, Object> dimValues = firstRecord.dimValues();
        List<String> dimFieldNames = dimValues.keySet().stream()
            .filter(field -> !field.equals("op_time"))
            .collect(Collectors.toList());

        // 构建CREATE TABLE语句
        StringBuilder sql = new StringBuilder();
        sql.append("CREATE TABLE ").append(tableName).append(" (")
           .append("kpi_id TEXT, ")
           .append("op_time TEXT, ");

        // 添加维度字段
        for (String dimField : dimFieldNames) {
            sql.append(dimField).append(" TEXT, ");
        }

        // 添加kpi_val字段
        sql.append("kpi_val TEXT)");

        try (Statement stmt = conn.createStatement()) {
            stmt.execute(sql.toString());
        }

        log.debug("创建表成功: {}", tableName);
    }

    /**
     * 插入数据到SQLite表
     */
    private void insertSQLiteData(Connection conn, String tableName,
                                  List<KpiComputeService.KpiDataRecord> records) throws SQLException {
        if (records.isEmpty()) {
            return;
        }

        // 获取维度字段名（排除op_time字段）
        KpiComputeService.KpiDataRecord firstRecord = records.get(0);
        Map<String, Object> dimValues = firstRecord.dimValues();
        List<String> dimFieldNames = dimValues.keySet().stream()
            .filter(field -> !field.equals("op_time"))
            .collect(Collectors.toList());

        // 构建INSERT语句
        StringBuilder sql = new StringBuilder();
        sql.append("INSERT INTO ").append(tableName)
           .append(" (kpi_id, op_time, ");

        // 添加维度字段
        for (String dimField : dimFieldNames) {
            sql.append(dimField).append(", ");
        }

        sql.append("kpi_val) VALUES (");

        // 添加占位符
        sql.append("?, ?, ");
        for (int i = 0; i < dimFieldNames.size(); i++) {
            sql.append("?, ");
        }
        sql.append("?)");

        String insertSql = sql.toString();

        try (PreparedStatement pstmt = conn.prepareStatement(insertSql)) {
            // 批量插入数据
            for (KpiComputeService.KpiDataRecord record : records) {
                int idx = 1;
                pstmt.setString(idx++, record.kpiId());
                pstmt.setString(idx++, record.opTime());

                // 设置维度值
                Map<String, Object> dimVals = record.dimValues();
                for (String dimField : dimFieldNames) {
                    Object dimVal = dimVals.get(dimField);
                    pstmt.setString(idx++, dimVal != null ? dimVal.toString() : null);
                }

                // 设置KPI值
                pstmt.setString(idx++, record.kpiVal() != null ? record.kpiVal().toString() : null);

                pstmt.addBatch();
            }

            pstmt.executeBatch();
        }

        log.debug("插入数据成功: {} 条记录", records.size());
    }

    /**
     * 根据批次时间获取周期类型
     */
    private String getCycleType(String opTime) {
        if (opTime.length() == 8) {
            return "DAY";
        } else if (opTime.length() == 6) {
            return "MONTH";
        } else if (opTime.length() == 4) {
            return "YEAR";
        }
        return "DAY";
    }

    /**
         * 存储结果
         */
        public record StorageResult(boolean success, String message, int storedCount) {

        public static StorageResult success(String message, int storedCount) {
                return new StorageResult(true, message, storedCount);
            }

            public static StorageResult error(String message) {
                return new StorageResult(false, message, 0);
            }
        }
}
