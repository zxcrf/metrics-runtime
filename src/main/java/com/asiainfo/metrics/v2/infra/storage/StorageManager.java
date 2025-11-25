package com.asiainfo.metrics.v2.infra.storage;

import com.asiainfo.metrics.config.MetricsConfig;
import com.asiainfo.metrics.service.MinIOService;
import com.asiainfo.metrics.v2.core.model.PhysicalTableReq;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
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
import java.util.zip.GZIPInputStream;

/**
 * 存储管理器 (Production Ready)
 * * 增强特性：
 * 1. 跨进程并发控制：使用 FileLock 解决多 JVM 部署时的竞态条件。
 * 2. 原子性保证：使用临时文件 + Atomic Move，防止读取到损坏/下载中的文件。
 * 3. 安全性：防止路径遍历攻击。
 * 4. 健壮性：自动清理残留的临时文件。
 */
@ApplicationScoped
public class StorageManager {

    private static final Logger log = LoggerFactory.getLogger(StorageManager.class);
    private static final String S3_FILE_NOT_EXISTS = "S3文件不存在";

    @Inject
    MinIOService minioService;

    @Inject
    MetricsConfig metricsConfig;

    // 单线程调度器用于后台清理，避免占用 HTTP 线程
    private final ScheduledExecutorService cleanupScheduler = Executors.newSingleThreadScheduledExecutor(r -> {
        Thread t = new Thread(r, "metrics-storage-cleaner");
        t.setDaemon(true);
        return t;
    });

    @PostConstruct
    void init() {
        long interval = metricsConfig.getStorageCleanupIntervalMinutes();
        log.info("初始化存储清理任务，最大容量: {}MB, 检查间隔: {}分钟",
                metricsConfig.getStorageMaxSizeMb(), interval);

        cleanupScheduler.scheduleWithFixedDelay(
                this::performCleanup,
                interval,
                interval,
                TimeUnit.MINUTES
        );
    }

    @PreDestroy
    void destroy() {
        cleanupScheduler.shutdownNow();
    }

    /**
     * 下载并准备物理表文件
     * 线程安全且进程安全 (Thread-safe & Process-safe)
     *
     * @param req 物理表请求对象
     * @return 本地可用的 .db 文件绝对路径
     */
    public String downloadAndPrepare(PhysicalTableReq req) {
        // 1. 安全检查：防止路径遍历
        validatePathSafe(req.kpiId());
        validatePathSafe(req.compDimCode());

        // 2. 构建路径信息
        String s3Key = buildS3Key(req.kpiId(), req.opTime(), req.compDimCode());
        String storageDir = metricsConfig.getSQLiteStorageDir();

        // 目标 .db 文件
        Path targetDbPath = Paths.get(storageDir, s3Key.replace(".gz", "")).toAbsolutePath();
        // 对应的锁文件 .lock
        Path lockFilePath = Paths.get(targetDbPath.toString() + ".lock");

        // 3. 快速检查：如果文件已存在且完整，直接返回 (Level 1 Check)
        // 注意：这里可能存在极低概率的竞态，但在大量读场景下性能提升显著
        if (Files.exists(targetDbPath)) {
            // 【关键】刷新文件时间戳，实现 LRU 保活
            touchFile(targetDbPath);
            return targetDbPath.toString();
        }

        // 确保父目录存在
        try {
            // 1. 快速检查 (Cache Hit L1)
            if (Files.exists(targetDbPath)) {
                // 【关键】刷新文件时间戳，实现 LRU 保活
                touchFile(targetDbPath);
                return targetDbPath.toString();
            }

            // 确保目录存在
            Files.createDirectories(targetDbPath.getParent());

            // 2. 获取锁并下载 (保留之前的 FileLock 逻辑)
            try (RandomAccessFile raf = new RandomAccessFile(lockFilePath.toFile(), "rw");
                 FileChannel channel = raf.getChannel()) {

                // 阻塞等待锁
                try (FileLock lock = channel.lock()) {
                    // Double Check
                    if (Files.exists(targetDbPath)) {
                        touchFile(targetDbPath);
                        return targetDbPath.toString();
                    }
                    return doDownloadAndDecompress(s3Key, targetDbPath);
                }
            }
        } catch (IOException e) {
            log.error("准备物理表失败: {}", s3Key, e);
            throw new RuntimeException("准备物理表失败: " + s3Key, e);
        }
    }

    /**
     * 刷新文件最后修改时间
     * 这会将文件标记为"最近使用"，防止被清理任务删除
     */
    private void touchFile(Path path) {
        try {
            // 更新 mtime 到当前时间
            Files.setLastModifiedTime(path, FileTime.from(Instant.now()));
        } catch (IOException e) {
            // 修改时间戳失败不应该阻断业务，记录 debug 日志即可
            log.debug("Failed to touch file: {}", path, e);
        }
    }

    /**
     * 执行磁盘清理 (LRU)
     */
    private void performCleanup() {
        String storageDir = metricsConfig.getSQLiteStorageDir();
        long maxSizeBytes = metricsConfig.getStorageMaxSizeMb() * 1024 * 1024;
        // 高水位阈值：当达到 max 时触发清理
        // 低水位目标：清理到 max * 0.8，避免频繁触发
        long targetSizeBytes = (long) (maxSizeBytes * 0.8);

        log.debug("开始执行磁盘清理检查...");

        try {
            Path rootDir = Paths.get(storageDir);
            if (!Files.exists(rootDir)) return;

            // 1. 扫描所有 .db 文件并统计总大小
            List<PathInfo> fileList = new ArrayList<>();
            long currentSize = 0;

            try (Stream<Path> stream = Files.walk(rootDir)) {
                // 必须收集到 List 中，因为后面要排序
                var paths = stream.filter(p -> {
                    String name = p.getFileName().toString();
                    return Files.isRegularFile(p) && (name.endsWith(".db") || name.endsWith(".db.gz"));
                }).toList();

                for (Path p : paths) {
                    try {
                        long size = Files.size(p);
                        currentSize += size;
                        // 获取最后修改时间 (mtime)
                        FileTime time = Files.getLastModifiedTime(p);
                        fileList.add(new PathInfo(p, size, time.toMillis()));
                    } catch (IOException ignored) {
                        // 文件可能被删除，跳过
                    }
                }
            }

            log.info("当前缓存大小: {} MB, 阈值: {} MB", currentSize / 1024 / 1024, maxSizeBytes / 1024 / 1024);

            if (currentSize <= maxSizeBytes) {
                return; // 未超限，无需清理
            }

            // 2. 按时间升序排序 (最旧的在前)
            fileList.sort(Comparator.comparingLong(o -> o.lastModified));

            // 3. 循环删除直到低于低水位
            long deletedCount = 0;
            long deletedBytes = 0;

            for (PathInfo info : fileList) {
                if (currentSize <= targetSizeBytes) break;

                try {
                    Files.deleteIfExists(info.path);
                    // 同时尝试删除对应的 .lock 文件
                    Files.deleteIfExists(Paths.get(info.path.toString() + ".lock"));

                    currentSize -= info.size;
                    deletedBytes += info.size;
                    deletedCount++;
                } catch (IOException e) {
                    log.warn("清理文件失败: {}", info.path);
                }
            }

            log.info("磁盘清理完成。删除了 {} 个文件，释放了 {} MB 空间", deletedCount, deletedBytes / 1024 / 1024);

        } catch (Exception e) {
            log.error("磁盘清理任务异常", e);
        }
    }

    // 辅助记录类
    private record PathInfo(Path path, long size, long lastModified) {}

    /**
     * 执行下载和解压逻辑
     * 使用临时文件策略确保原子性
     */
    private String doDownloadAndDecompress(String s3Key, Path targetPath) throws IOException {
        Path tempGzPath = null;
        Path tempDbPath = null;

        try {
            // 创建临时文件名 (e.g., file.db.tmp.123456)
            // 使用同一个目录，确保 atomic move 可行（跨分区 move 可能失败）
            tempGzPath = Files.createTempFile(targetPath.getParent(), "download_", ".gz");
            tempDbPath = Files.createTempFile(targetPath.getParent(), "decompress_", ".db");

            // A. 从 MinIO 下载到临时文件
            log.info("开始从MinIO下载: {}", s3Key);
            if (!minioService.statObject(s3Key)) {
                throw new RuntimeException(S3_FILE_NOT_EXISTS + ": " + s3Key);
            }
            minioService.downloadObject(s3Key, tempGzPath.toString());

            // B. 解压到临时 DB 文件
            log.info("正在解压: {} -> {}", tempGzPath, tempDbPath);
            decompressFile(tempGzPath, tempDbPath);

            // C. 原子移动 (Atomic Move)
            // 这是关键一步：将临时文件重命名为目标文件。
            // 在 POSIX 系统上这是原子的。如果目标文件被其他线程创建了，ATOMIC_MOVE 也会正确处理。
            Files.move(tempDbPath, targetPath, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
            log.info("文件准备完成: {}", targetPath);

            return targetPath.toString();

        } catch (Exception e) {
            log.error("处理文件失败，清理临时文件...", e);
            throw new RuntimeException("处理文件失败: " + s3Key, e);
        } finally {
            // 清理临时文件
            deleteQuietly(tempGzPath);
            // 如果成功移动，tempDbPath 就不存在了；如果失败，这里会清理残留
            deleteQuietly(tempDbPath);
        }
    }

    /**
     * 解压缩优化版
     */
    private void decompressFile(Path inputPath, Path outputPath) throws IOException {
        final int BUFFER_SIZE = 16 * 1024; // 16KB Buffer

        try (GZIPInputStream gzis = new GZIPInputStream(
                new BufferedInputStream(Files.newInputStream(inputPath), BUFFER_SIZE));
             BufferedOutputStream bos = new BufferedOutputStream(
                     new FileOutputStream(outputPath.toFile()), BUFFER_SIZE)) {

            byte[] buffer = new byte[BUFFER_SIZE];
            int len;
            while ((len = gzis.read(buffer)) > 0) {
                bos.write(buffer, 0, len);
            }
        }
    }

    /**
     * 构建 S3 存储路径 (保留原有逻辑)
     */
    private String buildS3Key(String kpiId, String opTime, String compDimCode) {
        String fileName = String.format("%s_%s_%s.db.gz", kpiId, opTime, compDimCode);
        String cleanTime = opTime.trim();

        List<String> pathParts = new ArrayList<>();
        if (cleanTime.length() == 8) {
            pathParts.add(cleanTime.substring(0, 4));
            pathParts.add(cleanTime.substring(0, 6));
            pathParts.add(cleanTime);
        } else if (cleanTime.length() == 6) {
            pathParts.add(cleanTime.substring(0, 4));
            pathParts.add(cleanTime);
        } else if (cleanTime.length() == 4) {
            pathParts.add(cleanTime);
        }

        String timePath = pathParts.isEmpty() ? cleanTime : String.join("/", pathParts);
        return String.format("%s/%s/%s", timePath, compDimCode, fileName);
    }

    /**
     * 安全检查：防止路径遍历攻击
     * 仅允许字母、数字、下划线、横线
     */
    private void validatePathSafe(String input) {
        if (input == null || !input.matches("^[a-zA-Z0-9_-]+$")) {
            throw new IllegalArgumentException("检测到非法路径字符: " + input);
        }
    }

    private void deleteQuietly(Path path) {
        if (path != null) {
            try {
                Files.deleteIfExists(path);
            } catch (IOException ignored) {
                // ignore
            }
        }
    }
}