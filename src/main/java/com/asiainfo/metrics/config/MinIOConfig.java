package com.asiainfo.metrics.config;

import io.minio.MinioClient;
import io.quarkus.runtime.StartupEvent;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Singleton;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * MinIO客户端配置
 */
@ApplicationScoped
public class MinIOConfig {

    private static final Logger log = LoggerFactory.getLogger(MinIOConfig.class);

    @ConfigProperty(name = "minio.endpoint", defaultValue = "http://localhost:9000")
    String endpoint;

    @ConfigProperty(name = "minio.access-key", defaultValue = "minioadmin")
    String accessKey;

    @ConfigProperty(name = "minio.secret-key", defaultValue = "minioadmin")
    String secretKey;

    @ConfigProperty(name = "minio.bucket.metrics", defaultValue = "metrics-runtime")
    String bucketName;

    @ConfigProperty(name = "minio.secure", defaultValue = "false")
    boolean secure;

    /**
     * 创建MinioClient Bean
     */
    @Produces
    @Singleton
    public MinioClient createMinioClient() {
        try {
            MinioClient minioClient = MinioClient.builder()
                .endpoint(endpoint)
                .credentials(accessKey, secretKey)
                .build();

            log.info("MinIO Client 初始化成功: {}", endpoint);
            return minioClient;
        } catch (Exception e) {
            log.error("MinIO Client 初始化失败", e);
            throw new RuntimeException("MinIO Client 初始化失败", e);
        }
    }

    void onStart(@Observes StartupEvent event) {
        log.info("MinIO配置初始化");
        log.info("Endpoint: {}", endpoint);
        log.info("Bucket: {}", bucketName);
        log.info("Secure: {}", secure);
    }

    public String getEndpoint() {
        return endpoint;
    }

    public String getAccessKey() {
        return accessKey;
    }

    public String getSecretKey() {
        return secretKey;
    }

    public String getBucketName() {
        return bucketName;
    }

    public boolean isSecure() {
        return secure;
    }
}
