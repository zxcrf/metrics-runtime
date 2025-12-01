package com.asiainfo.metrics.common.infrastructure.minio;

import com.asiainfo.metrics.common.config.MinIOConfig;
import io.minio.DownloadObjectArgs;
import io.minio.MinioClient;
import io.minio.StatObjectArgs;
import io.minio.UploadObjectArgs;
import io.minio.errors.ErrorResponseException;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.ConcurrentHashMap;

@ApplicationScoped
public class MinIOService {

    private static final Logger log = LoggerFactory.getLogger(MinIOService.class);

    // 简单的本地缓存，避免重复调用 OS 的 mkdirs
    private final ConcurrentHashMap<String, Boolean> dirCache = new ConcurrentHashMap<>();

    @Inject
    MinioClient minioClient;

    @Inject
    MinIOConfig minIOConfig;

    public void uploadResult(String localPath, String resultKey) throws IOException {
        try {
            minioClient.uploadObject(
                    UploadObjectArgs.builder()
                            .bucket(minIOConfig.getBucketName())
                            .object(resultKey)
                            .filename(localPath)
                            .contentType("application/octet-stream")
                            .build()
            );
            log.debug("上传MinIO成功: {}", resultKey); // 改为debug，减少高并发下的日志IO
        } catch (Exception e) {
            log.error("上传MinIO失败: {}", localPath, e);
            throw new IOException("上传结果文件失败", e);
        }
    }

    /**
     * 精确检查文件是否存在
     * 只捕获 'NoSuchKey' 错误，其他网络错误应抛出异常或返回 false 但记录日志
     */
    public boolean statObject(String key) {
        try {
            minioClient.statObject(
                    StatObjectArgs.builder()
                            .bucket(minIOConfig.getBucketName())
                            .object(key)
                            .build()
            );
            return true;
        } catch (ErrorResponseException e) {
            // 404 Not Found - 明确知道文件不存在
            if ("NoSuchKey".equals(e.errorResponse().code())) {
                return false;
            }
            log.warn("检查MinIO文件异常 [ErrorResponse]: key={}, code={}", key, e.errorResponse().code());
            return false;
        } catch (Exception e) {
            // 网络超时、认证失败等 - 视为"未知"，但在业务逻辑中通常只能当做不存在处理，但需要留痕
            log.warn("检查MinIO文件系统异常: key={}, msg={}", key, e.getMessage());
            return false;
        }
    }

    public void downloadObject(String s3Key, String localPath) throws IOException {
        try {
            ensureDirectoryExists(localPath);

            minioClient.downloadObject(
                    DownloadObjectArgs.builder()
                            .bucket(minIOConfig.getBucketName())
                            .object(s3Key)
                            .filename(localPath)
                            .overwrite(true)
                            .build()
            );

            // 使用 debug 级别，避免高并发下日志刷屏
            log.debug("下载成功: {}", s3Key);

        } catch (Exception e) {
            // 捕获所有 MinioException, IOException 等
            log.error("下载失败 [{}]: {}", s3Key, e.getMessage());
            throw new IOException("从MinIO下载文件失败: " + s3Key, e);
        }
    }

    // 减少高并发下的 IO 系统调用
    private void ensureDirectoryExists(String localPath) throws IOException {
        Path path = Paths.get(localPath).getParent();
        if (path == null) return;

        String dirKey = path.toString();
        if (!dirCache.containsKey(dirKey)) {
            if (!Files.exists(path)) {
                Files.createDirectories(path);
            }
            dirCache.put(dirKey, true);
        }
    }
}