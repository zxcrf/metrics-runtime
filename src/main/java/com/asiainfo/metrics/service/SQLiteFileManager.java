package com.asiainfo.metrics.service;

import com.asiainfo.metrics.config.MetricsConfig;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
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
import java.util.ArrayList;
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

    public static final String STR_DIM = "dim";
    public static final String STR_KPI = "kpi";
    public static final String STR_TARGET = "target";
    public static final String S3_FILE_NOT_EXISTS = "s3文件不存在";
    @Inject
    MinIOService minioService;

    @Inject
    MetricsConfig metricsConfig;

    public String createDBFile(String kpiId, String opTime, String compDimCode) throws IOException {
        String localPath = metricsConfig.getSQLiteStorageDir() + File.separator + buildS3Key(kpiId, opTime,  compDimCode);
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
     *
     * @param s3Key {op_time}/{compDimCode}/{kpi_id}/{kpi_id}_{op_time}_{compDimCode}.db.gz ..
     * localPath =  <dir>/<s3Key>
     * @return
     * @throws IOException
     */
    private String downloadAndCache(String s3Key) throws IOException {
        // 1. 检查本地文件是否已存在（解压后的.db文件）
        String localPath = metricsConfig.getSQLiteStorageDir() + File.separator + s3Key;
        String decompressedPath = localPath.replace(".gz", "");
        if (Files.exists(Paths.get(decompressedPath))) {
            log.info("使用本地SQLite缓存: {}", decompressedPath);
            return decompressedPath;
        }

        // 2. 检查压缩文件是否已存在（.db.gz文件）
        if (Files.exists(Paths.get(localPath))) {
            log.info("解压缩本地SQLite缓存: {}", localPath);
            return decompressFile(localPath);
        }

        // 3. 如果本地不存在，从MinIO下载（下载.db.gz文件）
        try {
            if (!minioService.statObject(s3Key)) {
                log.info("{}, key: {}", S3_FILE_NOT_EXISTS, s3Key);
                throw new RuntimeException(S3_FILE_NOT_EXISTS +", key: " + s3Key);
            }
            // 从MinIO下载压缩文件
            log.info("开始从MinIO下载SQLite文件: {}", s3Key);
            minioService.downloadObject(s3Key, localPath);

            log.info("开始解压缩SQLite文件: {}", localPath);
            return decompressFile(localPath);

        } catch (IOException e) {
            log.error("下载SQLite文件失败: {}", s3Key, e);
            throw new RuntimeException("下载SQLite文件失败", e);
        }
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
    public String downloadDataDB(String kpiId, String opTime, String compDimCode) throws IOException {
        String s3Key = buildS3Key(kpiId, opTime, compDimCode);
        return downloadAndCache(s3Key);
    }

    /**
     * 下载并缓存维度表SQLite文件
     * 优先检查本地缓存，如果不存在则从MinIO下载
     *
     * @param compDimCode 组合维度编码
     * @return 本地文件路径
     */
    public String downloadAndCacheDimDB(String compDimCode) throws IOException {
        String s3Key = String.format("%s/%s_%s_%s.db.gz",STR_DIM, STR_KPI, STR_DIM, compDimCode);
        return downloadAndCache(s3Key);
    }

    public String downloadAndCacheTargetDB(String compDimCode) throws IOException {
        String s3Key = String.format("%s/%s_%s_%s.db.gz", STR_TARGET, STR_KPI, STR_TARGET, compDimCode);
        return downloadAndCache(s3Key);
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
        KpiComputeService.KpiDataRecord firstRecord = records.getFirst();
        Map<String, Object> dimValues = firstRecord.dimValues();
        List<String> dimFieldNames = dimValues.keySet().stream()
            .filter(field -> !field.equals("op_time"))
            .toList();

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
        KpiComputeService.KpiDataRecord firstRecord = records.getFirst();
        Map<String, Object> dimValues = firstRecord.dimValues();
        List<String> dimFieldNames = dimValues.keySet().stream()
            .filter(field -> !field.equals("op_time"))
            .toList();

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
     * 构建S3存储键
     * 格式: {op_time}/{compDimCode}/{kpi_id}/{kpi_id}_{op_time}_{compDimCode}.db.gz
     */
    private String buildS3Key(String kpiId, String opTime, String compDimCode) {
        String fileName = String.format("%s_%s_%s.db.gz", kpiId, opTime, compDimCode);

        String cleanStr = opTime.trim();
        List<String> pathParts = new ArrayList<>();
        String finalOpTime;
        if(cleanStr.length() == 8) {
            pathParts.add(cleanStr.substring(0, 4));
            pathParts.add(cleanStr.substring(0, 6));
            pathParts.add(cleanStr);
        }else if(cleanStr.length() == 6) {
            pathParts.add(cleanStr.substring(0, 4));
            pathParts.add(cleanStr);
        }else if(cleanStr.length() == 4) {
            pathParts.add(cleanStr);
        }
        finalOpTime = pathParts.isEmpty() ? cleanStr : String.join("/", pathParts);

        return String.format("%s/%s/%s", finalOpTime, compDimCode, fileName);
    }

    /**
     * kpi_KD1002_20251104_CD003
     */
    public String getSQLiteTableName(String kpiId, String opTime, String compDimCode) {
        return String.format("%s_%s_%s_%s", STR_KPI, kpiId, opTime, compDimCode);
    }

    /**
     * kpi_dim_CD003
     */
    public String getSQLiteDimTableName(String compDimCode) {
        return String.format("%s_%s_%s", STR_KPI, STR_DIM, compDimCode).intern();
    }

    /**
     * kpi_target_value_CD003
     */
    public String getSQLiteTargetTableName(String compDimCode) {
        return "kpi_target_value_"+compDimCode;
    }
}
