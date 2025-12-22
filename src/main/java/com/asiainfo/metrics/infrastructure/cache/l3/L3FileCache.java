package com.asiainfo.metrics.infrastructure.cache.l3;

import com.asiainfo.metrics.infrastructure.config.MetricsConfig;
import com.asiainfo.metrics.infrastructure.storage.MinIOService;
import com.asiainfo.metrics.infrastructure.cache.CacheConfig;
import com.asiainfo.metrics.domain.model.PhysicalTableReq;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.stream.Stream;

/**
 * L3 文件缓存
 * 管理本地Parquet文件缓存
 */
@ApplicationScoped
public class L3FileCache {

    private static final Logger log = LoggerFactory.getLogger(L3FileCache.class);

    @Inject
    CacheConfig cacheConfig;

    @Inject
    MetricsConfig metricsConfig;

    @Inject
    MinIOService minioService;

    /**
     * 获取文件（如果不存在则下载）
     * 
     * @param req 物理表请求
     * @return 本地文件路径
     */
    public String getOrDownload(PhysicalTableReq req) throws IOException {
        String s3Key = buildS3Key(req.kpiId(), req.opTime(), req.compDimCode());

        if (!cacheConfig.isL3Enabled()) {
            // L3禁用时，直接下载到临时文件
            return downloadToTemp(s3Key);
        }

        // L3启用时，使用本地缓存
        Path localPath = getLocalPath(s3Key);
        if (Files.exists(localPath)) {
            log.debug("[L3 Cache] Hit: {}", s3Key);
            return localPath.toString();
        }

        // 下载到缓存目录
        return downloadToCache(s3Key, localPath);
    }

    /**
     * 失效（删除）指定文件
     */
    public void invalidate(String kpiId, String opTime) {
        if (!cacheConfig.isL3Enabled()) {
            return; // L3禁用时不需要清理
        }

        try {
            Path cacheDir = getCacheDir();
            String pattern = String.format("kpi_%s_%s_*.parquet", kpiId, opTime);

            deleteMatchingFiles(cacheDir, pattern);
        } catch (Exception e) {
            log.warn("[L3 Cache] Invalidation failed for kpi={}, opTime={}: {}",
                    kpiId, opTime, e.getMessage());
        }
    }

    /**
     * 下载到临时文件（L3禁用时）
     */
    private String downloadToTemp(String s3Key) throws IOException {
        Path tempFile = Files.createTempFile("minio_nocache_", ".parquet");
        log.info("[L3 Cache Disabled] Downloading to temp: {}", s3Key);

        minioService.downloadObject(s3Key, tempFile.toString());
        return tempFile.toString();
    }

    /**
     * 下载到缓存目录（L3启用时）
     */
    private String downloadToCache(String s3Key, Path localPath) throws IOException {
        log.info("[L3 Cache] Downloading: {}", s3Key);
        Files.createDirectories(localPath.getParent());

        // 使用临时文件原子移动
        Path tempPath = Files.createTempFile(localPath.getParent(), "download_", ".tmp");
        minioService.downloadObject(s3Key, tempPath.toString());
        Files.move(tempPath, localPath, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);

        return localPath.toString();
    }

    /**
     * 获取本地缓存路径
     */
    private Path getLocalPath(String s3Key) {
        String storageDir = metricsConfig.getSQLiteStorageDir();
        return Paths.get(storageDir, s3Key).toAbsolutePath();
    }

    /**
     * 获取缓存目录
     */
    private Path getCacheDir() {
        return Paths.get(metricsConfig.getSQLiteStorageDir());
    }

    /**
     * 删除匹配的文件
     */
    private void deleteMatchingFiles(Path dir, String pattern) throws IOException {
        try (Stream<Path> paths = Files.walk(dir)) {
            long count = paths
                    .filter(p -> p.getFileName().toString().matches(pattern.replace("*", ".*")))
                    .peek(p -> {
                        try {
                            Files.deleteIfExists(p);
                            log.debug("[L3 Cache] Deleted: {}", p);
                        } catch (IOException e) {
                            log.warn("[L3 Cache] Failed to delete: {}", p);
                        }
                    })
                    .count();

            if (count > 0) {
                log.info("[L3 Cache] Invalidated {} files", count);
            }
        }
    }

    /**
     * 构建S3 Key
     */
    private String buildS3Key(String kpiId, String opTime, String compDimCode) {
        String fileName = String.format("kpi_%s_%s_%s.parquet", kpiId, opTime, compDimCode);
        String cleanTime = opTime.trim();

        if (cleanTime.length() == 8) {
            String year = cleanTime.substring(0, 4);
            String yearMonth = cleanTime.substring(0, 6);
            return String.format("%s/%s/%s/%s/%s", year, yearMonth, cleanTime, compDimCode, fileName);
        }

        return String.format("%s/%s/%s", cleanTime, compDimCode, fileName);
    }
}
