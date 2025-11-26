package com.asiainfo.metrics.etl;

import io.minio.MinioClient;
import io.minio.UploadObjectArgs;
import okhttp3.ConnectionPool;
import okhttp3.OkHttpClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.*;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.zip.GZIPOutputStream;

/**
 * SQLite 数据生产工具 (ETL Producer)
 * 用于将源数据库(MySQL/Oracle)的数据抽取并转换为适配 Metrics Runtime 的 SQLite 文件
 */
public class SqliteDataProducer {

    private static final Logger log = LoggerFactory.getLogger(SqliteDataProducer.class);

    private final String bucketName;
    private final MinioClient minioClient;

    public SqliteDataProducer(String endpoint, String accessKey, String secretKey, String bucket) {
        this.bucketName = bucket;

        // 1. 关键修复：配置 OkHttp 连接池
        // 服务端(MinIO)可能配置了较短的 Keep-Alive 时间（如 60s），如果客户端连接池默认保持 5 分钟，
        // 就会导致客户端尝试复用一个已被服务端关闭的连接，从而报 EOFException。
        // 解决方案：设置较短的 keepAliveDuration (如 30s)，让客户端主动回收空闲连接。
        OkHttpClient httpClient = new OkHttpClient.Builder()
                .connectionPool(new ConnectionPool(5, 30, TimeUnit.SECONDS))
                .connectTimeout(10, TimeUnit.SECONDS)
                .writeTimeout(60, TimeUnit.SECONDS) // 上传大文件需要较长的写超时
                .readTimeout(30, TimeUnit.SECONDS)
                .retryOnConnectionFailure(true)
                .build();

        this.minioClient = MinioClient.builder()
                .endpoint(endpoint)
                .credentials(accessKey, secretKey)
                .httpClient(httpClient)
                .build();

        log.info("SqliteDataProducer 初始化完成 (Keep-Alive: 30s)");
    }

    /**
     * 生产 KPI 数据文件
     * 对应 Runtime 的物理表: kpi_{kpiId}_{opTime}_{compDimCode}
     *
     * @param sourceConn    源数据库连接
     * @param sourceSql     源数据查询SQL (必须包含 dim_cols 和 kpi_val)
     * @param kpiId         指标编码
     * @param opTime        时间账期 (yyyyMMdd)
     * @param compDimCode   组合维度编码
     * @param dimCols       维度列名列表 (如 city_id, county_id)
     */
    public void produceKpiData(Connection sourceConn, String sourceSql,
                               String kpiId, String opTime, String compDimCode,
                               List<String> dimCols) throws Exception {

        // 1. 准备本地临时文件
        String tableName = String.format("kpi_%s_%s_%s", kpiId, opTime, compDimCode);
        File dbFile = File.createTempFile(tableName, ".db");
        String dbPath = dbFile.getAbsolutePath();

        log.info("开始生产 KPI 数据: {} -> {}", tableName, dbPath);

        // 2. 创建 SQLite 表结构并导入数据
        try (Connection sqliteConn = DriverManager.getConnection("jdbc:sqlite:" + dbPath);
             Statement sqliteStmt = sqliteConn.createStatement();
             PreparedStatement sourceStmt = sourceConn.prepareStatement(sourceSql);
             ResultSet sourceRs = sourceStmt.executeQuery()) {

            // 2.1 建表
            // 必须包含: kpi_id, op_time, kpi_val 以及所有维度列
            StringBuilder createSql = new StringBuilder();
            createSql.append("CREATE TABLE ").append(tableName).append(" (");
            createSql.append("kpi_id TEXT, op_time TEXT, kpi_val REAL");
            for (String dim : dimCols) {
                createSql.append(", ").append(dim).append(" TEXT");
            }
            createSql.append(")");
            sqliteStmt.execute(createSql.toString());

            // 2.2 插入数据
            sqliteConn.setAutoCommit(false);
            StringBuilder insertSql = new StringBuilder();
            insertSql.append("INSERT INTO ").append(tableName).append(" VALUES (?, ?, ?");
            for (int i = 0; i < dimCols.size(); i++) insertSql.append(", ?");
            insertSql.append(")");

            try (PreparedStatement insertStmt = sqliteConn.prepareStatement(insertSql.toString())) {
                int count = 0;
                while (sourceRs.next()) {
                    // 固定字段
                    insertStmt.setString(1, kpiId);
                    insertStmt.setString(2, opTime);
                    // 指标值 (假设源SQL中列名为 kpi_val)
                    insertStmt.setDouble(3, sourceRs.getDouble("kpi_val"));

                    // 动态维度字段
                    for (int i = 0; i < dimCols.size(); i++) {
                        // 假设源SQL中的列名与 dimCols 一致
                        insertStmt.setString(4 + i, sourceRs.getString(dimCols.get(i)));
                    }

                    insertStmt.addBatch();
                    if (++count % 5000 == 0) insertStmt.executeBatch();
                }
                insertStmt.executeBatch();
                sqliteConn.commit();
                log.info("数据导入完成，共 {} 条", count);
            }

            // 2.3 创建索引 (优化查询性能)
            // 为维度列创建索引
            if (!dimCols.isEmpty()) {
                String idxCols = String.join(", ", dimCols);
                sqliteStmt.execute("CREATE INDEX idx_" + tableName + " ON " + tableName + "(" + idxCols + ")");
            }
        }

        // 3. 压缩文件 (.db -> .db.gz)
        String gzPath = dbPath + ".gz";
        compressGzip(dbPath, gzPath);

        // 4. 上传到 MinIO
        // 路径规范: {year}/{yyyymm}/{yyyymmdd}/{compDimCode}/{filename}
        String s3Key = buildKpiS3Key(kpiId, opTime, compDimCode);
        uploadToMinio(gzPath, s3Key);

        // 5. 清理临时文件
        Files.deleteIfExists(Paths.get(dbPath));
        Files.deleteIfExists(Paths.get(gzPath));
        log.info("生产完成，文件已上传至: {}", s3Key);
    }

    /**
     * 生产维度表数据
     * 对应 Runtime 的维度表: kpi_dim_{compDimCode}
     */
    public void produceDimData(Connection sourceConn, String sourceSql,
                               String compDimCode, List<String> dimCols) throws Exception {

        String tableName = "kpi_dim_" + compDimCode;
        File dbFile = File.createTempFile(tableName, ".db");
        String dbPath = dbFile.getAbsolutePath();

        log.info("开始生产维度数据: {} -> {}", tableName, dbPath);

        try (Connection sqliteConn = DriverManager.getConnection("jdbc:sqlite:" + dbPath);
             Statement sqliteStmt = sqliteConn.createStatement();
             PreparedStatement sourceStmt = sourceConn.prepareStatement(sourceSql);
             ResultSet sourceRs = sourceStmt.executeQuery()) {

            // 2.1 建表
            // 必须包含: dim_code, dim_val, 以及所有维度ID和描述 (e.g., city_id, city_id_desc)
            String createSql = "CREATE TABLE " + tableName + " (" +
                    "dim_code TEXT, dim_val TEXT, dim_id text, parent_dim_code text )"; // 联合主键和联合描述
            sqliteStmt.execute(createSql);

            // 2.2 插入数据
            sqliteConn.setAutoCommit(false);
            StringBuilder insertSql = new StringBuilder();
            insertSql.append("INSERT INTO ").append(tableName).append(" (dim_code, dim_val, dim_id, parent_dim_code) VALUES (?, ?, ?, ?)");

            try (PreparedStatement insertStmt = sqliteConn.prepareStatement(insertSql.toString())) {
                int count = 0;
                while (sourceRs.next()) {
                    // 假设源SQL已经按照 dim_code, dim_val, col1, col1_desc... 的顺序查好了
                    // 或者在这里做简单的拼接逻辑
                    insertStmt.setString(1, sourceRs.getString("dim_code"));
                    insertStmt.setString(2, sourceRs.getString("dim_val"));
                    insertStmt.setString(3, sourceRs.getString("dim_id"));
                    insertStmt.setString(4, sourceRs.getString("parent_dim_code"));

                    insertStmt.addBatch();
                    if (++count % 5000 == 0) insertStmt.executeBatch();
                }
                insertStmt.executeBatch();
                sqliteConn.commit();
            }

            // 创建索引
            sqliteStmt.execute("CREATE INDEX idx_dim_code ON " + tableName + "(dim_code)");
        }

        // 压缩和上传
        String gzPath = dbPath + ".gz";
        compressGzip(dbPath, gzPath);

        // 维度表路径: dim/kpi_dim_{compDimCode}.db.gz
        String s3Key = String.format("dim/kpi_dim_%s.db.gz", compDimCode);
        uploadToMinio(gzPath, s3Key);

        Files.deleteIfExists(Paths.get(dbPath));
        Files.deleteIfExists(Paths.get(gzPath));
    }

    // --- 辅助方法 ---

    private void compressGzip(String src, String dst) throws IOException {
        try (FileInputStream fis = new FileInputStream(src);
             GZIPOutputStream gos = new GZIPOutputStream(new FileOutputStream(dst))) {
            byte[] buffer = new byte[1024];
            int len;
            while ((len = fis.read(buffer)) > 0) {
                gos.write(buffer, 0, len);
            }
        }
    }

    private void uploadToMinio(String filePath, String objectKey) throws Exception {
        // 2. 关键修复：应用层重试机制
        // 即使配置了连接池，网络抖动仍不可避免。显式重试能极大提高 ETL 任务的成功率。
        int maxRetries = 3;
        for (int i = 0; i < maxRetries; i++) {
            try {
                log.info("正在上传到 MinIO (第 {}/{} 次): key={}", i + 1, maxRetries, objectKey);
                minioClient.uploadObject(UploadObjectArgs.builder()
                        .bucket(bucketName)
                        .object(objectKey)
                        .filename(filePath)
                        .build());
                log.info("上传成功: {}", objectKey);
                return; // 成功则返回
            } catch (Exception e) {
                log.warn("上传失败: {}", e.getMessage());
                if (i == maxRetries - 1) {
                    throw e; // 最后一次尝试失败，抛出异常
                }
                // 退避策略：等待 1s, 2s...
                Thread.sleep(1000L * (i + 1));
            }
        }
    }

    private String buildKpiS3Key(String kpiId, String opTime, String compDimCode) {
        String fileName = String.format("%s_%s_%s.db.gz", kpiId, opTime, compDimCode);
        // 构建时间路径: yyyy/yyyymm/yyyymmdd
        String timePath;
        if (opTime.length() == 8) {
            timePath = opTime.substring(0, 4) + "/" + opTime.substring(0, 6) + "/" + opTime;
        } else {
            timePath = opTime; // 简单回退
        }
        return String.format("%s/%s/%s", timePath, compDimCode, fileName);
    }
}