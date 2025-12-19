package com.asiainfo.metrics.v2.infrastructure.repository;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.inject.Named;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.*;
import java.time.LocalDateTime;
import java.util.Optional;

/**
 * ETL表到达日志 Repository
 * 管理 metrics_etl_log 表，实现 UPSERT 语义
 */
@ApplicationScoped
public class ETLLogRepository {

    private static final Logger log = LoggerFactory.getLogger(ETLLogRepository.class);

    @Inject
    @Named("metadb")
    DataSource dataSource;

    /**
     * 记录或更新表到达时间（UPSERT语义）
     * 首次到达：INSERT
     * 再次到达：UPDATE arrival_time（标记ETL重跑）
     */
    public void logTableArrival(String tableName, String opTime) {
        // 使用 MySQL 的 ON DUPLICATE KEY UPDATE 实现 UPSERT
        String sql = """
            INSERT INTO metrics_etl_log (table_name, op_time, arrival_time)
            VALUES (?, ?, NOW())
            ON DUPLICATE KEY UPDATE arrival_time = NOW()
            """;

        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {

            stmt.setString(1, tableName);
            stmt.setString(2, opTime);
            int affected = stmt.executeUpdate();

            log.info("[ETL Log] Table arrival logged: table={}, opTime={}, affected={}", 
                    tableName, opTime, affected);

        } catch (SQLException e) {
            log.error("[ETL Log] Failed to log table arrival: table={}, opTime={}", 
                    tableName, opTime, e);
            throw new RuntimeException("Failed to log table arrival", e);
        }
    }

    /**
     * 检查指定表+批次是否已到达
     */
    public boolean exists(String tableName, String opTime) {
        String sql = "SELECT COUNT(*) FROM metrics_etl_log WHERE table_name = ? AND op_time = ?";

        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {

            stmt.setString(1, tableName);
            stmt.setString(2, opTime);

            try (ResultSet rs = stmt.executeQuery()) {
                return rs.next() && rs.getInt(1) > 0;
            }

        } catch (SQLException e) {
            log.error("[ETL Log] Failed to check existence: table={}, opTime={}", 
                    tableName, opTime, e);
            return false;
        }
    }

    /**
     * 获取到达时间（用于重跑检测）
     */
    public Optional<LocalDateTime> getArrivalTime(String tableName, String opTime) {
        String sql = "SELECT arrival_time FROM metrics_etl_log WHERE table_name = ? AND op_time = ?";

        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {

            stmt.setString(1, tableName);
            stmt.setString(2, opTime);

            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    Timestamp ts = rs.getTimestamp("arrival_time");
                    return Optional.ofNullable(ts != null ? ts.toLocalDateTime() : null);
                }
            }

        } catch (SQLException e) {
            log.error("[ETL Log] Failed to get arrival time: table={}, opTime={}", 
                    tableName, opTime, e);
        }

        return Optional.empty();
    }
}
