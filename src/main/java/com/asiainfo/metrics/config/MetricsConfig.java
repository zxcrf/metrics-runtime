package com.asiainfo.metrics.config;

import jakarta.enterprise.context.ApplicationScoped;
import org.eclipse.microprofile.config.ConfigProvider;

/**
 * 指标系统配置
 *
 * @author QvQ
 * @date 2025/11/12
 */
@ApplicationScoped
public class MetricsConfig {

    /**
     * 获取当前使用的存储计算引擎类型
     * 可能的值：MYSQL, SQLITE
     */
    public String getCurrentEngine() {
        return ConfigProvider.getConfig()
                .getValue("metrics.engine.type", String.class);
    }

    /**
     * 获取SQLite文件存储目录
     */
    public String getSQLiteStorageDir() {
        return ConfigProvider.getConfig()
                .getValue("metrics.sqlite.storage.dir", String.class);
    }

    /**
     * 获取MinIO存储桶名称
     */
    public String getMinioBucketName() {
        return ConfigProvider.getConfig()
                .getValue("minio.bucket.metrics", String.class);
    }

    /**
     * 获取是否启用SQLite模式
     */
    public boolean isSQLiteEnabled() {
        return "SQLite".equalsIgnoreCase(getCurrentEngine());
    }

    /**
     * 获取是否启用MySQL模式
     */
    public boolean isMySQLEnabled() {
        String engine = getCurrentEngine();
        return "MySQL".equalsIgnoreCase(engine) || engine.isEmpty();
    }
}
