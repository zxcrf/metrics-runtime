package com.asiainfo.metrics.config;

import io.agroal.api.AgroalDataSource;
import io.quarkus.runtime.StartupEvent;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.Statement;

/**
 * SQLite 配置类
 * 为内存数据库提供最优配置
 *
 * 注意：SQLite配置通过application.properties中的JDBC URL参数进行，
 * 不在代码中执行PRAGMA命令，以避免SQL解析错误
 */
@ApplicationScoped
public class SQLiteConfig {

    private static final Logger log = LoggerFactory.getLogger(SQLiteConfig.class);

    @Inject
    @io.quarkus.agroal.DataSource("sqlite")
    AgroalDataSource sqliteDataSource;

    /**
     * 验证SQLite连接是否正常
     */
    void onStart(@Observes StartupEvent event) {
        try (Connection conn = sqliteDataSource.getConnection();
             Statement stmt = conn.createStatement()) {

            // 验证SQLite版本
            String version = stmt.executeQuery("SELECT sqlite_version()").getString(1);
            log.info("SQLite 版本: {}", version);

            // 验证数据库连接正常
            stmt.execute("SELECT 1");

            log.info("SQLite 内存数据库连接正常");

        } catch (Exception e) {
            log.error("SQLite 配置初始化失败", e);
            throw new RuntimeException("SQLite 配置初始化失败", e);
        }
    }
}
