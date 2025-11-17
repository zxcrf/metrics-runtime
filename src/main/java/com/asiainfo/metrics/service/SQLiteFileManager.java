package com.asiainfo.metrics.service;

import com.asiainfo.metrics.config.MetricsConfig;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Map;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

/**
 * SQLite 文件管理器
 * 负责下载、缓存和管理SQLite文件
 *
 * 特点:
 * - 自动下载和缓存到PVC挂载路径
 * - 多实例共享缓存
 * - 支持压缩/解压缩
 */
@ApplicationScoped
public class SQLiteFileManager {

    private static final Logger log = LoggerFactory.getLogger(SQLiteFileManager.class);

    @Inject
    MinIOService minioService;

    @Inject
    MetricsConfig metricsConfig;

//    @Inject
//    @CacheName("sqlite-files")
//    Cache sqliteFileCache;

    public String createDBFile(String kpiId, String opTime, String compDimCode) throws IOException {
        String localPath = buildLocalPath(kpiId, opTime, compDimCode);
        //创建文件夹
        Path path = Paths.get(localPath);
        if(!path.getParent().toFile().exists()) {
            try{
                path.getParent().toFile().mkdirs();
            }catch (Exception e) {
                throw new IOException(e);
            }
        }
        //文件若存在，则先删除
        if (Files.exists(path)) {
            Files.delete(path);
        }

        Files.createFile(path);
        return localPath;
    }

    /**
     * 下载并缓存SQLite文件
     * 优先检查本地缓存，如果不存在则从MinIO下载
     *
     * @param kpiId KPI编码
     * @param opTime 运营时间
     * @param compDimCode 组合维度编码
     * @return 本地文件路径
     */
    public String downloadAndCacheDB(String kpiId, String opTime, String compDimCode) throws IOException {
        String cacheKey = buildCacheKey(kpiId, opTime, compDimCode);

        // 1. 检查本地文件是否已存在（解压后的.db文件）
        String localPath = buildLocalPath(kpiId, opTime, compDimCode);

        if (Files.exists(Paths.get(localPath))) {
            log.info("使用本地SQLite缓存: {}", localPath);
            return localPath;
        }

        // 2. 检查压缩文件是否已存在（.db.gz文件）
        String compressedPath = localPath + ".gz";
        if (Files.exists(Paths.get(compressedPath))) {
            log.info("解压缩本地SQLite缓存: {}", compressedPath);
            return decompressFile(compressedPath);
        }

        // 3. 如果本地不存在，从MinIO下载（下载.db.gz文件）
        try {
            // 构建MinIO上的压缩文件路径
            String s3Key = String.format("metrics/%s/%s/%s/%s_%s_%s.db.gz",
                opTime, compDimCode, kpiId, kpiId, opTime, compDimCode);

            // 从MinIO下载压缩文件
            log.info("从MinIO下载SQLite文件: {}", s3Key);
            minioService.downloadObject(s3Key, compressedPath);

            log.info("下载并解压缩SQLite文件成功: {}", cacheKey);

            // 解压缩文件
            return decompressFile(compressedPath);

        } catch (IOException e) {
            log.error("下载SQLite文件失败: {}", cacheKey, e);
            throw new RuntimeException("下载SQLite文件失败", e);
        }
    }

    /**
     * 创建SQLite表结构
     *
     * @param conn 数据库连接
     * @param tableName 表名
     * @param records 数据记录（用于获取维度字段）
     * @throws SQLException SQL异常
     */
    public void createSQLiteTable(Connection conn, String tableName,
                                   List<KpiComputeService.KpiDataRecord> records) throws SQLException {
        if (records == null || records.isEmpty()) {
            return;
        }

        // 获取维度字段名（排除op_time字段）
        KpiComputeService.KpiDataRecord firstRecord = records.get(0);
        Map<String, Object> dimValues = firstRecord.dimValues();
        List<String> dimFieldNames = dimValues.keySet().stream()
            .filter(field -> !field.equals("op_time"))
            .collect(java.util.stream.Collectors.toList());

        // 构建CREATE TABLE语句（使用IF NOT EXISTS避免表已存在错误）
        // 添加主键约束保证幂等性：(kpi_id, op_time, 维度字段)
        StringBuilder sql = new StringBuilder();
        sql.append("CREATE TABLE IF NOT EXISTS ").append(tableName).append(" (")
           .append("kpi_id TEXT, ")
           .append("op_time TEXT, ");

        // 添加维度字段
        for (String dimField : dimFieldNames) {
            sql.append(dimField).append(" TEXT, ");
        }

        // 添加kpi_val字段
        sql.append("kpi_val TEXT, ");

        // 构建主键字段列表
        sql.append("PRIMARY KEY (kpi_id, op_time");
        for (String dimField : dimFieldNames) {
            sql.append(", ").append(dimField);
        }
        sql.append("))");

        try (Statement stmt = conn.createStatement()) {
            stmt.execute(sql.toString());
        }

        log.debug("创建表成功: {}", tableName);
    }

    /**
     * 插入数据到SQLite表
     *
     * @param conn 数据库连接
     * @param tableName 表名
     * @param records 数据记录列表
     * @throws SQLException SQL异常
     */
    public void insertSQLiteData(Connection conn, String tableName,
                                  List<KpiComputeService.KpiDataRecord> records) throws SQLException {
        if (records == null || records.isEmpty()) {
            return;
        }

        // 获取维度字段名（排除op_time字段）
        KpiComputeService.KpiDataRecord firstRecord = records.get(0);
        Map<String, Object> dimValues = firstRecord.dimValues();
        List<String> dimFieldNames = dimValues.keySet().stream()
            .filter(field -> !field.equals("op_time"))
            .collect(java.util.stream.Collectors.toList());

        // 构建INSERT OR REPLACE语句（保证幂等性）
        // 如果主键冲突，自动替换旧记录
        StringBuilder sql = new StringBuilder();
        sql.append("INSERT OR REPLACE INTO ").append(tableName)
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
     * 上传计算结果
     *
     * @param localPath 本地文件路径
     * @param metricName 指标名称 (KPI ID)
     * @param timeRange 时间范围 (操作时间)
     * @param compDimCode 组合维度编码
     */
    public void uploadResultDB(String localPath, String metricName, String timeRange, String compDimCode) throws IOException {
        String resultKey = buildS3Key(metricName, timeRange, compDimCode);

        try {
            // 先压缩文件
            String compressedPath = compressFile(localPath);
            minioService.uploadResult(compressedPath, resultKey);

            log.info("上传计算结果成功: {}", resultKey);

        } catch (IOException e) {
            log.error("上传计算结果失败: {}", localPath, e);
            throw e;
        }
    }

    /**
     * 压缩文件
     */
    private String compressFile(String inputPath) throws IOException {
        String outputPath = inputPath + ".gz";

        try (FileInputStream fis = new FileInputStream(inputPath);
             GZIPOutputStream gzos = new GZIPOutputStream(
                 Files.newOutputStream(Paths.get(outputPath)))) {

            byte[] buffer = new byte[8192];
            int bytesRead;
            while ((bytesRead = fis.read(buffer)) != -1) {
                gzos.write(buffer, 0, bytesRead);
            }
        }

        log.debug("文件压缩完成: {} -> {}", inputPath, outputPath);
        return outputPath;
    }

    /**
     * 解压缩文件
     */
    public String decompressFile(String compressedPath) throws IOException {
        String outputPath = compressedPath.replace(".gz", "");

        try (GZIPInputStream gzis = new GZIPInputStream(
                 Files.newInputStream(Paths.get(compressedPath)));
             FileOutputStream fos = new FileOutputStream(outputPath)) {

            byte[] buffer = new byte[8192];
            int bytesRead;
            while ((bytesRead = gzis.read(buffer)) != -1) {
                fos.write(buffer, 0, bytesRead);
            }
        }

        log.debug("文件解压缩完成: {} -> {}", compressedPath, outputPath);
        return outputPath;
    }

    /**
     * 构建缓存键
     */
    private String buildCacheKey(String kpiId, String opTime, String compDimCode) {
        return kpiId + "_" + opTime + "_" + compDimCode;
    }

    /**
     * 构建S3存储键
     * 格式: metrics/{op_time}/{compDimCode}/{kpi_id}/{kpi_id}_{op_time}_{compDimCode}.db.gz
     */
    private String buildS3Key(String kpiId, String opTime, String compDimCode) {
        String fileName = String.format("%s_%s_%s.db.gz", kpiId, opTime, compDimCode);
        return String.format("metrics/%s/%s/%s/%s", opTime, compDimCode, kpiId, fileName);
    }

    /**
     * 构建本地缓存路径
     */
    private String buildLocalPath(String kpiId, String opTime, String compDimCode) {
        return String.format("%s/%s_%s_%s.db", metricsConfig.getSQLiteStorageDir(), kpiId, opTime, compDimCode);
    }

    public String getSQLiteTableName(String kpiId, String opTime, String compDimCode) {
        return String.format("kpi_%s_%s_%s", kpiId, opTime, compDimCode);
    }

    /**
     * 清理过期缓存
     */
    public void cleanupCache() {
        log.info("开始清理SQLite文件缓存...");
        // 缓存自动过期，这里可以添加手动清理逻辑
    }
}
