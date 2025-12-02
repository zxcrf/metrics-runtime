package com.asiainfo.metrics.v2.infrastructure.storage;

import com.asiainfo.metrics.common.config.MetricsConfig;
import com.asiainfo.metrics.common.infrastructure.minio.MinIOService;
import com.asiainfo.metrics.v2.domain.model.PhysicalTableReq;
import com.asiainfo.metrics.v2.infrastructure.cache.CacheManager;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.FileTime;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

/**
 * 存储管理器 (Production Ready - Parquet Only)
 * 移除运行时格式转换，直接下载 Parquet 文件。
 */
@ApplicationScoped
public class StorageManager {

    private static final Logger log = LoggerFactory.getLogger(StorageManager.class);

    @Inject
    MinIOService minioService;

    @Inject
    MetricsConfig metricsConfig;

    @Inject
    MeterRegistry registry;

    @Inject
    CacheManager cacheManager;

    // 单线程调度器用于后台清理
    private final ScheduledExecutorService cleanupScheduler = Executors.newSingleThreadScheduledExecutor(r -> {
        Thread t = new Thread(r, "metrics-storage-cleaner");
        t.setDaemon(true);
        return t;
    });

    @PostConstruct
    void init() {
        long interval = metricsConfig.getStorageCleanupIntervalMinutes();
        cleanupScheduler.scheduleWithFixedDelay(
                this::performCleanup,
                interval,
                interval,
                TimeUnit.MINUTES);
    }

    @PreDestroy
    void destroy() {
        cleanupScheduler.shutdownNow();
    }

    /**
     * 下载并缓存维度表
     */
    public String downloadAndCacheDimDB(String compDimCode) throws Exception {
        validatePathSafe(compDimCode);
        // 假设维度表也已在 ETL 端转换为 Parquet
        String s3Key = String.format("dim/kpi_dim_%s.parquet", compDimCode);

        return Timer.builder("metrics.storage.download.time")
                .tag("type", "dim")
                .register(registry)
                .recordCallable(() -> downloadWithLock(s3Key));
    }

    /**
     * 下载并准备物理表文件
     */
    public String downloadAndPrepare(PhysicalTableReq req) throws Exception {
        validatePathSafe(req.kpiId());
        validatePathSafe(req.compDimCode());

        return Timer.builder("metrics.storage.download.time")
                .tag("type", "kpi")
                .register(registry)
                .recordCallable(() -> cacheManager.getFile(req));
    }

    // 使用 synchronized 保护下载过程
    private final java.util.concurrent.ConcurrentHashMap<String, Object> fileLocks = new java.util.concurrent.ConcurrentHashMap<>();

    // IO 优化：缓存文件存在性
    private final java.util.concurrent.ConcurrentHashMap<String, Boolean> existenceCache = new java.util.concurrent.ConcurrentHashMap<>();
    private final java.util.concurrent.ConcurrentHashMap<String, Long> lastTouchCache = new java.util.concurrent.ConcurrentHashMap<>();
    private static final long TOUCH_INTERVAL_MS = 60_000L;

    /**
     * 通用下载逻辑 (Parquet Only)
     */
    private String downloadWithLock(String parquetS3Key) {
        long t0 = System.currentTimeMillis();

        String storageDir = metricsConfig.getSQLiteStorageDir();
        Path targetPath = Paths.get(storageDir, parquetS3Key).toAbsolutePath();

        // Note: L3 cache logic now handled by L3FileCache through CacheManager
        // This method is kept for dimension table downloads and legacy compatibility

        // 以下是 L3 缓存启用时的逻辑

        // 1. Fast Path: 缓存命中
        if (existenceCache.containsKey(targetPath.toString()) || Files.exists(targetPath)) {
            existenceCache.put(targetPath.toString(), Boolean.TRUE);
            touchFile(targetPath);
            log.debug("[L3 Cache] File cache hit: {}", parquetS3Key);
            return targetPath.toString();
        }

        // 2. Slow Path: 下载并保存到本地
        Object javaLock = fileLocks.computeIfAbsent(targetPath.toString(), k -> new Object());

        synchronized (javaLock) {
            // Double check
            if (Files.exists(targetPath)) {
                existenceCache.put(targetPath.toString(), Boolean.TRUE);
                touchFile(targetPath);
                return targetPath.toString();
            }

            try {
                log.info("[Storage] Downloading Parquet: {}", parquetS3Key);
                Files.createDirectories(targetPath.getParent());

                // 使用临时文件原子移动
                Path tempPath = Files.createTempFile(targetPath.getParent(), "download_", ".tmp");

                // 下载
                minioService.downloadObject(parquetS3Key, tempPath.toString());

                // Atomic move
                Files.move(tempPath, targetPath, StandardCopyOption.ATOMIC_MOVE,
                        StandardCopyOption.REPLACE_EXISTING);

                long elapsed = System.currentTimeMillis() - t0;
                log.info("[Storage] Downloaded {} in {}ms", parquetS3Key, elapsed);

                existenceCache.put(targetPath.toString(), Boolean.TRUE);
                touchFile(targetPath);
                return targetPath.toString();

            } catch (IOException e) {
                log.error("[Storage] Download failed: {}", parquetS3Key, e);
                // 检查是否是文件不存在错误
                if (isFileNotExistError(e)) {
                    throw new FileNotExistException("File does not exist in MinIO: " + parquetS3Key, parquetS3Key, e);
                }
                throw new RuntimeException("Failed to download file: " + parquetS3Key, e);
            }
        }
    }

    /**
     * L3 缓存禁用时：直接从 MinIO 下载到临时文件，不保存到本地缓存目录
     * 每次都是全新下载，确保测试的是真实的 MinIO + 查询性能
     */
    private String downloadFromMinIODirectly(String parquetS3Key, long t0) {
        try {
            log.info("[L3 Cache Disabled] Downloading from MinIO directly: {}", parquetS3Key);

            // 创建临时文件（不在缓存目录中）
            Path tempPath = Files.createTempFile("minio_nocache_", ".parquet");

            // 从 MinIO 下载
            minioService.downloadObject(parquetS3Key, tempPath.toString());

            long elapsed = System.currentTimeMillis() - t0;
            log.info("[L3 Cache Disabled] Downloaded {} in {}ms to temp file", parquetS3Key, elapsed);

            // 返回临时文件路径
            // 注意：这个临时文件在 JVM 退出时会被删除，或者依赖操作系统清理
            return tempPath.toString();

        } catch (IOException e) {
            log.error("[L3 Cache Disabled] Download failed: {}", parquetS3Key, e);
            if (isFileNotExistError(e)) {
                throw new FileNotExistException("File does not exist in MinIO: " + parquetS3Key, parquetS3Key, e);
            }
            throw new RuntimeException("Failed to download file: " + parquetS3Key, e);
        }
    }

    private void touchFile(Path path) {
        String key = path.toString();
        long now = System.currentTimeMillis();
        Long lastTouch = lastTouchCache.get(key);

        if (lastTouch != null && (now - lastTouch) < TOUCH_INTERVAL_MS) {
            return;
        }

        try {
            Files.setLastModifiedTime(path, FileTime.from(Instant.now()));
            lastTouchCache.put(key, now);
        } catch (IOException e) {
            // ignore
        }
    }

    private void performCleanup() {
        String storageDir = metricsConfig.getSQLiteStorageDir();
        long maxSizeBytes = metricsConfig.getStorageMaxSizeMb() * 1024 * 1024;
        long targetSizeBytes = (long) (maxSizeBytes * 0.8);

        try {
            Path rootDir = Paths.get(storageDir);
            if (!Files.exists(rootDir))
                return;

            List<PathInfo> fileList = new ArrayList<>();
            long currentSize = 0;

            try (Stream<Path> stream = Files.walk(rootDir)) {
                var paths = stream.filter(p -> {
                    String name = p.getFileName().toString();
                    return Files.isRegularFile(p) && name.endsWith(".parquet");
                }).toList();

                for (Path p : paths) {
                    try {
                        long size = Files.size(p);
                        currentSize += size;
                        FileTime time = Files.getLastModifiedTime(p);
                        fileList.add(new PathInfo(p, size, time.toMillis()));
                    } catch (IOException ignored) {
                    }
                }
            }

            if (currentSize <= maxSizeBytes)
                return;

            fileList.sort(Comparator.comparingLong(o -> o.lastModified));

            long deletedCount = 0;
            for (PathInfo info : fileList) {
                if (currentSize <= targetSizeBytes)
                    break;
                if (fileLocks.containsKey(info.path.toString()))
                    continue;

                try {
                    Files.deleteIfExists(info.path);
                    existenceCache.remove(info.path.toString());
                    currentSize -= info.size;
                    deletedCount++;
                } catch (IOException e) {
                    log.warn("Delete failed: {}", info.path);
                }
            }
            log.info("Cleanup finished. Deleted {} files.", deletedCount);

        } catch (Exception e) {
            log.error("Cleanup task error", e);
        }
    }

    private record PathInfo(Path path, long size, long lastModified) {
    }

    private String buildS3Key(String kpiId, String opTime, String compDimCode) {
        // 始终构造 Parquet 路径
        // 格式:
        // {year}/{yearmonth}/{date}/{compDimCode}/{kpi_id}_{op_time}_{compDimCode}.parquet
        String fileName = String.format("kpi_%s_%s_%s.parquet", kpiId, opTime, compDimCode);
        String cleanTime = opTime.trim();

        List<String> pathParts = new ArrayList<>();
        if (cleanTime.length() == 8) {
            pathParts.add(cleanTime.substring(0, 4));
            pathParts.add(cleanTime.substring(0, 6));
            pathParts.add(cleanTime);
        } else {
            pathParts.add(cleanTime);
        }

        String timePath = String.join("/", pathParts);
        return String.format("%s/%s/%s", timePath, compDimCode, fileName);
    }

    private void validatePathSafe(String input) {
        if (input == null || !input.matches("^[a-zA-Z0-9_-]+$")) {
            throw new IllegalArgumentException("Invalid path characters: " + input);
        }
    }

    /**
     * 检查是否是文件不存在错误
     */
    private boolean isFileNotExistError(IOException e) {
        // 检查消息
        String msg = e.getMessage();
        if (msg != null && (msg.contains("NoSuchKey")
                || msg.contains("does not exist")
                || msg.contains("Object does not exist")
                || msg.contains("The specified key does not exist"))) {
            return true;
        }

        // 检查异常链中是否有 MinIO ErrorResponseException
        Throwable cause = e.getCause();
        while (cause != null) {
            String causeMsg = cause.getMessage();
            String causeClass = cause.getClass().getName();

            // 检查是否是 MinIO 的 ErrorResponseException 且消息包含 "does not exist"
            if (causeClass.contains("ErrorResponseException") && causeMsg != null
                    && causeMsg.contains("does not exist")) {
                return true;
            }

            cause = cause.getCause();
        }

        return false;
    }

    /**
     * 自定义异常：文件不存在
     */
    public static class FileNotExistException extends RuntimeException {
        private final String s3Key;

        public FileNotExistException(String message, String s3Key, Throwable cause) {
            super(message, cause);
            this.s3Key = s3Key;
        }

        public String getS3Key() {
            return s3Key;
        }

        /**
         * 从 S3 Key 中解析出 KpiId 和 OpTime
         * S3 Key 格式:
         * {year}/{yearmonth}/{date}/{compDimCode}/kpi_{kpiId}_{opTime}_{compDimCode}.parquet
         */
        public String extractKpiId() {
            try {
                String fileName = s3Key.substring(s3Key.lastIndexOf('/') + 1);
                // kpi_{kpiId}_{opTime}_{compDimCode}.parquet
                if (fileName.startsWith("kpi_")) {
                    String[] parts = fileName.split("_");
                    if (parts.length >= 3) {
                        return parts[1]; // kpiId
                    }
                }
            } catch (Exception ignored) {
            }
            return s3Key; // fallback
        }

        public String extractOpTime() {
            try {
                String fileName = s3Key.substring(s3Key.lastIndexOf('/') + 1);
                if (fileName.startsWith("kpi_")) {
                    String[] parts = fileName.split("_");
                    if (parts.length >= 3) {
                        return parts[2]; // opTime
                    }
                }
            } catch (Exception ignored) {
            }
            return "unknown";
        }
    }
}