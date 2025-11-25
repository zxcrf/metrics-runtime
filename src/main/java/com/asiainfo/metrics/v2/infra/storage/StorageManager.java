package com.asiainfo.metrics.v2.infra.storage;

import com.asiainfo.metrics.config.MetricsConfig;
import com.asiainfo.metrics.service.MinIOService;
import com.asiainfo.metrics.v2.core.model.PhysicalTableReq;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.file.*;
import java.util.ArrayList;
import java.util.List;
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
            return targetDbPath.toString();
        }

        // 确保父目录存在
        try {
            Files.createDirectories(targetDbPath.getParent());
        } catch (IOException e) {
            throw new RuntimeException("无法创建目录: " + targetDbPath.getParent(), e);
        }

        // 4. 获取文件锁，进入临界区
        // 使用 try-with-resources 确保 Channel 关闭
        try (RandomAccessFile raf = new RandomAccessFile(lockFilePath.toFile(), "rw");
             FileChannel channel = raf.getChannel()) {

            log.debug("正在等待文件锁: {}", lockFilePath);
            // lock() 是阻塞的，直到获取到独占锁。OS 保证跨进程互斥。
            try (FileLock lock = channel.lock()) {

                // 5. 双重检查 (Double-Check): 获取锁后再次检查文件是否已被其他进程下载好
                if (Files.exists(targetDbPath)) {
                    log.debug("获取锁后发现文件已就绪: {}", targetDbPath);
                    return targetDbPath.toString();
                }

                // 6. 执行核心逻辑：下载 -> 解压 -> 原子移动
                return doDownloadAndDecompress(s3Key, targetDbPath);
            }

        } catch (IOException e) {
            log.error("文件锁处理失败: {}", lockFilePath, e);
            throw new RuntimeException("获取文件锁失败: " + req.kpiId(), e);
        }
        // 锁会在 FileLock close 或 JVM 关闭时自动释放
    }

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