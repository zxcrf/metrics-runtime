package com.asiainfo.metrics.service;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
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

//    @Inject
//    @CacheName("sqlite-files")
//    Cache sqliteFileCache;

    /**
     * 下载并缓存SQLite文件
     * 优先检查本地缓存，如果不存在则从MinIO下载
     *
     * @param metricName 指标名称
     * @param timeRange 时间范围
     * @return 本地文件路径
     */
    public String downloadAndCacheDB(String metricName, String timeRange) throws IOException {
        String cacheKey = buildCacheKey(metricName, timeRange);
        try {
            String downloadedPath = minioService.downloadToLocal(metricName, timeRange);
            log.info("下载并缓存SQLite文件成功: {}", cacheKey);
            return downloadedPath;
        } catch (IOException e) {
            log.error("下载SQLite文件失败: {}", cacheKey, e);
            throw new RuntimeException("下载SQLite文件失败", e);
        }
    }

    /**
     * 上传计算结果
     *
     * @param localPath 本地文件路径
     * @param metricName 指标名称
     * @param timeRange 时间范围
     */
    public void uploadResultDB(String localPath, String metricName, String timeRange) throws IOException {
        String resultKey = buildS3Key(metricName, timeRange);

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
    private String buildCacheKey(String metricName, String timeRange) {
        return metricName + "_" + timeRange;
    }

    /**
     * 构建S3存储键
     */
    private String buildS3Key(String metricName, String timeRange) {
        return String.format("metrics/%s/%s.db.gz", metricName, timeRange);
    }

    /**
     * 构建本地缓存路径
     */
    private String buildLocalPath(String metricName, String timeRange) {
        return String.format("/tmp/cache/%s_%s.db", metricName, timeRange);
    }

    /**
     * 清理过期缓存
     */
    public void cleanupCache() {
        log.info("开始清理SQLite文件缓存...");
        // 缓存自动过期，这里可以添加手动清理逻辑
    }
}
