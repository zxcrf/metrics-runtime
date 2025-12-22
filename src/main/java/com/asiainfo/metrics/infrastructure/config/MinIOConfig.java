package com.asiainfo.metrics.infrastructure.config;

import io.minio.MinioClient;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Singleton;
import okhttp3.ConnectionPool;
import okhttp3.Dispatcher;
import okhttp3.OkHttpClient;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

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

    public String getBucketName() {
        return bucketName;
    }

    // --- 【极速小文件模式配置】 ---

    // 1. 关键优化：最大空闲连接数
    // 场景：300KB文件下载只需几毫秒。瞬间并发500个请求，几毫秒后全部归还。
    // 如果 MaxIdle < MaxRequests，多余的连接会被立即销毁，导致 CPU 浪费在 TCP 挥手上。
    // 策略：让 MaxIdle >= MaxRequestsPerHost，确保连接 100% 复用，零销毁。
    @ConfigProperty(name = "minio.pool.max-idle", defaultValue = "500")
    int maxIdleConnections;

    // 2. 存活时间
    // 既然请求极其频繁，连接保持活跃的时间可以缩短，防止长期闲置占用资源
    @ConfigProperty(name = "minio.pool.keep-alive-minutes", defaultValue = "2")
    long keepAliveDurationMinutes;

    // 3. 并发数 (QPS 吞吐量)
    // 200KB 的文件，单机千兆网卡一秒能处理几千个。
    // 瓶颈通常在 MinIO 服务端的 QPS 处理能力，而不是客户端。
    // 建议设置较高，让虚拟线程跑满。
    @ConfigProperty(name = "minio.http.max-requests", defaultValue = "2000")
    int maxRequests;

    @ConfigProperty(name = "minio.http.max-requests-per-host", defaultValue = "500")
    int maxRequestsPerHost;

    // 4. 超时控制 (Fail-Fast)
    // 300KB 文件在内网传输也就 10ms。
    // 如果 5秒 还没读完，说明网络断了或者 MinIO 死了，立即报错重试，别占着线程。
    @ConfigProperty(name = "minio.http.read-timeout-sec", defaultValue = "5")
    long readTimeoutSec;

    @ConfigProperty(name = "minio.http.connect-timeout-sec", defaultValue = "2")
    long connectTimeoutSec;

    @Produces
    @Singleton
    public MinioClient createMinioClient() {
        try {
            Dispatcher dispatcher = new Dispatcher();
            dispatcher.setMaxRequests(maxRequests);
            dispatcher.setMaxRequestsPerHost(maxRequestsPerHost);

            OkHttpClient httpClient = new OkHttpClient.Builder()
                    .dispatcher(dispatcher)
                    .connectionPool(new ConnectionPool(
                            maxIdleConnections, // 核心：设为 500
                            keepAliveDurationMinutes,
                            TimeUnit.MINUTES))
                    // 极速超时策略
                    .connectTimeout(connectTimeoutSec, TimeUnit.SECONDS) // 2秒连不上就报错
                    .readTimeout(readTimeoutSec, TimeUnit.SECONDS)       // 5秒读不完就报错
                    .writeTimeout(5, TimeUnit.SECONDS)
                    .retryOnConnectionFailure(true)
                    .build();

            return MinioClient.builder()
                    .endpoint(endpoint)
                    .credentials(accessKey, secretKey)
                    .httpClient(httpClient)
                    .build();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}