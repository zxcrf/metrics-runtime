package com.asiainfo.metrics.service;

import com.asiainfo.metrics.config.MinIOConfig;
import io.minio.DownloadObjectArgs;
import io.minio.MinioClient;
import io.minio.StatObjectArgs;
import io.minio.UploadObjectArgs;
import io.minio.errors.MinioException;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;

/**
 * MinIO S3 服务
 * 负责SQLite文件的下载和上传
 */
@ApplicationScoped
public class MinIOService {

    private static final Logger log = LoggerFactory.getLogger(MinIOService.class);

    @Inject
    MinioClient minioClient;

    @Inject
    MinIOConfig minIOConfig;

    /**
     * 上传计算结果到MinIO
     *
     * @param localPath 本地文件路径
     * @param resultKey S3存储键
     */
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

            log.info("上传结果到MinIO成功: {} -> {}", localPath, resultKey);

        } catch (MinioException | InvalidKeyException | NoSuchAlgorithmException e) {
            log.error("上传结果到MinIO失败: {}", localPath, e);
            throw new IOException("上传结果文件失败", e);
        }
    }

    /**
     * 检查S3文件是否存在
     */
    public boolean statObject(String key) {
        try {
            return minioClient.statObject(
                StatObjectArgs.builder()
                    .bucket(minIOConfig.getBucketName())
                    .object(key)
                    .build()
            ) != null;
        } catch (Exception e) {
            return false;
        }
    }

    /**
     * 从MinIO下载文件到本地（自定义路径）
     *
     * @param s3Key MinIO中的对象键
     * @param localPath 本地文件路径
     * @throws IOException 下载失败
     */
    public void downloadObject(String s3Key, String localPath) throws IOException {
        try {
            // 确保本地目录存在
            Path localDir = Paths.get(localPath).getParent();
            Files.createDirectories(localDir);

            // 从MinIO下载文件
            minioClient.downloadObject(
                DownloadObjectArgs.builder()
                    .bucket(minIOConfig.getBucketName())
                    .object(s3Key)
                    .filename(localPath)
                    .build()
            );

            log.info("从MinIO下载文件成功: {} -> {}", s3Key, localPath);

        } catch (MinioException | InvalidKeyException | NoSuchAlgorithmException e) {
            log.error("从MinIO下载文件失败: {}", s3Key, e);
            throw new IOException("下载文件失败", e);
        }
    }

}
