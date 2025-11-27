package com.asiainfo.metrics.service;

import com.asiainfo.metrics.config.MetricsConfig;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Parquet文件管理器
 * 负责使用DuckDB直接生成Parquet文件并上传到MinIO
 *
 * @author QvQ
 * @date 2025/11/27
 */
@ApplicationScoped
public class ParquetFileManager {

    private static final Logger log = LoggerFactory.getLogger(ParquetFileManager.class);

    public static final String STR_DIM = "dim";
    public static final String STR_KPI = "kpi";
    public static final String STR_TARGET = "target";

    @Inject
    MinIOService minioService;

    @Inject
    MetricsConfig metricsConfig;

    /**
     * 创建Parquet文件路径
     */
    public String createParquetFilePath(String kpiId, String opTime, String compDimCode) throws IOException {
        String localPath = metricsConfig.getSQLiteStorageDir() + File.separator
                + buildS3Key(kpiId, opTime, compDimCode).replace(".db.gz", ".parquet");

        // 创建文件夹
        Path path = Paths.get(localPath);
        if (!path.getParent().toFile().exists()) {
            path.getParent().toFile().mkdirs();
        }

        // 文件若存在，则先删除
        if (Files.exists(path)) {
            Files.delete(path);
        }

        return localPath;
    }

    /**
     * 使用DuckDB生成Parquet文件
     * * @param localPath 本地Parquet文件路径
     * @param tableName 临时表名
     * @param records   数据记录列表
     * @throws SQLException SQL异常
     */
    public void writeDataToParquet(String localPath, String tableName,
                                   List<KpiComputeService.KpiDataRecord> records) throws SQLException {
        if (records == null || records.isEmpty()) {
            return;
        }

        // 使用DuckDB内存数据库
        try (Connection conn = DriverManager.getConnection("jdbc:duckdb:")) {
            // 1. 创建临时表
            createDuckDBTable(conn, tableName, records);

            // 2. 插入数据
            insertDuckDBData(conn, tableName, records);

            // 3. 导出为Parquet
            exportToParquet(conn, tableName, localPath);

            log.info("Parquet文件生成成功: {}, 记录数: {}", localPath, records.size());
        }
    }

    /**
     * 使用DuckDB生成维度表Parquet文件
     *
     * @param localPath 本地Parquet文件路径
     * @param tableName 临时表名
     * @param records   维度数据记录列表
     * @throws SQLException SQL异常
     */
    public void writeDimDataToParquet(String localPath, String tableName,
                                      List<Map<String, String>> records) throws SQLException {
        if (records == null || records.isEmpty()) {
            return;
        }

        try (Connection conn = DriverManager.getConnection("jdbc:duckdb:")) {
            // 1. 创建维度表结构
            try (Statement stmt = conn.createStatement()) {
                stmt.execute("CREATE TABLE " + tableName + " (dim_code VARCHAR, dim_val VARCHAR, dim_id VARCHAR, parent_dim_code VARCHAR)");
            }

            // 2. 插入数据
            String insertSql = "INSERT INTO " + tableName + " VALUES (?, ?, ?, ?)";
            try (PreparedStatement ps = conn.prepareStatement(insertSql)) {
                for (Map<String, String> row : records) {
                    ps.setString(1, row.get("dim_code"));
                    ps.setString(2, row.get("dim_val"));
                    ps.setString(3, row.get("dim_id"));
                    ps.setString(4, row.get("parent_dim_code"));
                    ps.addBatch();
                }
                ps.executeBatch();
            }

            // 3. 导出为Parquet
            exportToParquet(conn, tableName, localPath);

            log.info("维度Parquet文件生成成功: {}, 记录数: {}", localPath, records.size());
        }
    }

    /**
     * 创建DuckDB表结构
     */
    private void createDuckDBTable(Connection conn, String tableName,
                                   List<KpiComputeService.KpiDataRecord> records) throws SQLException {
        KpiComputeService.KpiDataRecord firstRecord = records.get(0);
        Map<String, Object> dimValues = firstRecord.dimValues();
        List<String> dimFieldNames = dimValues.keySet().stream()
                .filter(field -> !field.equals("op_time"))
                .sorted()
                .toList();

        // 构建CREATE TABLE语句
        StringBuilder sql = new StringBuilder();
        sql.append("CREATE TABLE ").append(tableName).append(" (")
                .append("kpi_id VARCHAR, ")
                .append("op_time VARCHAR, ");

        // 添加维度字段
        for (String dimField : dimFieldNames) {
            sql.append(dimField).append(" VARCHAR, ");
        }

        // 添加kpi_val字段 (修改为 DOUBLE)
        sql.append("kpi_val DOUBLE)");

        try (Statement stmt = conn.createStatement()) {
            stmt.execute(sql.toString());
        }

        log.debug("DuckDB表创建成功: {}", tableName);
    }

    /**
     * 插入数据到DuckDB表
     */
    private void insertDuckDBData(Connection conn, String tableName,
                                  List<KpiComputeService.KpiDataRecord> records) throws SQLException {
        KpiComputeService.KpiDataRecord firstRecord = records.get(0);
        Map<String, Object> dimValues = firstRecord.dimValues();
        List<String> dimFieldNames = dimValues.keySet().stream()
                .filter(field -> !field.equals("op_time"))
                .sorted()
                .toList();

        // 构建INSERT语句
        StringBuilder sql = new StringBuilder();
        sql.append("INSERT INTO ").append(tableName)
                .append(" (kpi_id, op_time, ");

        for (String dimField : dimFieldNames) {
            sql.append(dimField).append(", ");
        }

        sql.append("kpi_val) VALUES (");

        // 添加占位符
        sql.append("?, ?, ");
        sql.append("?, ".repeat(dimFieldNames.size()));
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

                // 设置KPI值 (解析为 Double)
                Object kpiVal = record.kpiVal();
                if (kpiVal == null) {
                    pstmt.setObject(idx++, null);
                } else {
                    // 兼容 String, Number 等类型，统一转 Double
                    pstmt.setDouble(idx++, Double.parseDouble(kpiVal.toString()));
                }

                pstmt.addBatch();
            }

            pstmt.executeBatch();
        }

        log.debug("数据插入成功: {} 条记录", records.size());
    }

    /**
     * 使用DuckDB导出为Parquet文件
     */
    private void exportToParquet(Connection conn, String tableName, String parquetPath) throws SQLException {
        String copySQL = String.format(
                "COPY %s TO '%s' (FORMAT PARQUET)",
                tableName,
                parquetPath);

        try (Statement stmt = conn.createStatement()) {
            stmt.execute(copySQL);
        }

        log.debug("Parquet导出成功: {}", parquetPath);
    }

    /**
     * 上传Parquet文件到MinIO
     */
    public void uploadParquetFile(String localPath, String kpiId, String opTime, String compDimCode)
            throws IOException {
        String s3Key = buildS3Key(kpiId, opTime, compDimCode).replace(".db.gz", ".parquet");

        try {
            minioService.uploadResult(localPath, s3Key);
            log.info("Parquet文件上传成功: {}", s3Key);
        } catch (IOException e) {
            log.error("Parquet文件上传失败: {}", localPath, e);
            throw e;
        }
    }

    /**
     * 构建S3存储键
     */
    private String buildS3Key(String kpiId, String opTime, String compDimCode) {
        String fileName = String.format("kpi_%s_%s_%s.db.gz", kpiId, opTime, compDimCode);

        String cleanStr = opTime.trim();
        List<String> pathParts = new ArrayList<>();
        if (cleanStr.length() == 8) {
            pathParts.add(cleanStr.substring(0, 4));
            pathParts.add(cleanStr.substring(0, 6));
            pathParts.add(cleanStr);
        } else if (cleanStr.length() == 6) {
            pathParts.add(cleanStr.substring(0, 4));
            pathParts.add(cleanStr);
        } else if (cleanStr.length() == 4) {
            pathParts.add(cleanStr);
        }
        String finalOpTime = pathParts.isEmpty() ? cleanStr : String.join("/", pathParts);

        return String.format("%s/%s/%s", finalOpTime, compDimCode, fileName);
    }

    /**
     * 生成Parquet表名
     */
    public String getParquetTableName(String kpiId, String opTime, String compDimCode) {
        return String.format("%s_%s_%s_%s", STR_KPI, kpiId, opTime, compDimCode);
    }

    /**
     * 创建维度表Parquet文件路径
     */
    public String createDimParquetFilePath(String compDimCode) throws IOException {
        String localPath = metricsConfig.getSQLiteStorageDir() + File.separator
                + String.format("%s/%s_%s_%s.parquet", STR_DIM, STR_KPI, STR_DIM, compDimCode);

        Path path = Paths.get(localPath);
        if (!path.getParent().toFile().exists()) {
            path.getParent().toFile().mkdirs();
        }

        if (Files.exists(path)) {
            Files.delete(path);
        }

        return localPath;
    }

    /**
     * 上传维度表Parquet文件到MinIO
     */
    public void uploadDimParquetFile(String localPath, String compDimCode) throws IOException {
        String s3Key = String.format("%s/%s_%s_%s.parquet", STR_DIM, STR_KPI, STR_DIM, compDimCode);

        try {
            minioService.uploadResult(localPath, s3Key);
            log.info("维度表Parquet文件上传成功: {}", s3Key);
        } catch (IOException e) {
            log.error("维度表Parquet文件上传失败: {}", localPath, e);
            throw e;
        }
    }
}