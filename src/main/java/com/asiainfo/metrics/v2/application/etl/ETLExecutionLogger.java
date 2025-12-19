package com.asiainfo.metrics.v2.application.etl;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.inject.Named;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.*;
import java.time.Instant;

/**
 * ETL执行日志记录器
 * 记录每次ETL执行的详细信息，支持重做检测
 */
@ApplicationScoped
public class ETLExecutionLogger {

    private static final Logger log = LoggerFactory.getLogger(ETLExecutionLogger.class);

    @Inject
    @Named("metadb")
    DataSource metaDataSource;

    /**
     * 记录ETL开始执行
     * 
     * @return 执行记录ID
     */
    public Long logStart(String kpiModelId, String opTime, String operator) {
        String executionType = checkExecutionType(kpiModelId, opTime);

        String sql = "INSERT INTO kpi_etl_execution_log " +
                "(kpi_model_id, op_time, execution_type, status, started_at, operator) " +
                "VALUES (?, ?, ?, 'RUNNING', NOW(), ?)";

        try (Connection conn = metaDataSource.getConnection();
                PreparedStatement stmt = conn.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS)) {

            stmt.setString(1, kpiModelId);
            stmt.setString(2, opTime);
            stmt.setString(3, executionType);
            stmt.setString(4, operator);

            stmt.executeUpdate();

            try (ResultSet rs = stmt.getGeneratedKeys()) {
                if (rs.next()) {
                    Long id = rs.getLong(1);
                    log.info("[ETL Log] Started: id={}, model={}, opTime={}, type={}",
                            id, kpiModelId, opTime, executionType);
                    return id;
                }
            }
        } catch (SQLException e) {
            log.error("[ETL Log] Failed to log start: {}", e.getMessage(), e);
        }
        return null;
    }

    /**
     * 记录ETL成功完成
     */
    public void logSuccess(Long executionId, int recordCount, long executionTimeMs) {
        if (executionId == null)
            return;

        String sql = "UPDATE kpi_etl_execution_log SET " +
                "status='SUCCESS', record_count=?, execution_time_ms=?, completed_at=NOW() " +
                "WHERE id=?";

        try (Connection conn = metaDataSource.getConnection();
                PreparedStatement stmt = conn.prepareStatement(sql)) {

            stmt.setInt(1, recordCount);
            stmt.setLong(2, executionTimeMs);
            stmt.setLong(3, executionId);

            stmt.executeUpdate();
            log.info("[ETL Log] Success: id={}, records={}, time={}ms",
                    executionId, recordCount, executionTimeMs);
        } catch (SQLException e) {
            log.error("[ETL Log] Failed to log success: {}", e.getMessage(), e);
        }
    }

    /**
     * 记录ETL失败
     */
    public void logFailure(Long executionId, String errorMessage) {
        if (executionId == null)
            return;

        String sql = "UPDATE kpi_etl_execution_log SET " +
                "status='FAILED', error_message=?, completed_at=NOW() " +
                "WHERE id=?";

        try (Connection conn = metaDataSource.getConnection();
                PreparedStatement stmt = conn.prepareStatement(sql)) {

            stmt.setString(1, errorMessage);
            stmt.setLong(2, executionId);

            stmt.executeUpdate();
            log.warn("[ETL Log] Failed: id={}, error={}", executionId, errorMessage);
        } catch (SQLException e) {
            log.error("[ETL Log] Failed to log failure: {}", e.getMessage(), e);
        }
    }

    /**
     * 检查执行类型：首次还是重做
     */
    private String checkExecutionType(String kpiModelId, String opTime) {
        String sql = "SELECT COUNT(*) FROM kpi_etl_execution_log " +
                "WHERE kpi_model_id=? AND op_time=? AND status='SUCCESS'";

        try (Connection conn = metaDataSource.getConnection();
                PreparedStatement stmt = conn.prepareStatement(sql)) {

            stmt.setString(1, kpiModelId);
            stmt.setString(2, opTime);

            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next() && rs.getInt(1) > 0) {
                    return "REDO";
                }
            }
        } catch (SQLException e) {
            log.warn("[ETL Log] Failed to check execution type: {}", e.getMessage());
        }

        return "INITIAL";
    }

    /**
     * ETL日志记录
     */
    public record ETLLogRecord(
            Long id,
            String kpiModelId,
            String opTime,
            String executionType,
            String status,
            Integer recordCount,
            String errorMessage,
            Long executionTimeMs,
            Timestamp startedAt,
            Timestamp completedAt,
            String operator,
            String remark) {
    }
}
