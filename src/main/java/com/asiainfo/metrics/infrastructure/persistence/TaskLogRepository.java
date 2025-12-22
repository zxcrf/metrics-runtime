package com.asiainfo.metrics.infrastructure.persistence;

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
 * 任务执行日志 Repository
 * 管理 metrics_task_log 表，支持并发控制和重跑检测
 */
@ApplicationScoped
public class TaskLogRepository {

    private static final Logger log = LoggerFactory.getLogger(TaskLogRepository.class);

    @Inject
    @Named("metadb")
    DataSource dataSource;

    /**
     * 任务日志记录
     */
    public record TaskLog(
            Long id,
            String modelId,
            String opTime,
            String status,
            LocalDateTime startTime,
            LocalDateTime endTime,
            String message,
            Integer computeCount,
            Integer storageCount
    ) {}

    /**
     * 创建RUNNING状态的任务记录
     * 
     * @return 任务记录ID
     */
    public Long createRunningTask(String modelId, String opTime) {
        String sql = """
            INSERT INTO metrics_task_log (model_id, op_time, status, start_time)
            VALUES (?, ?, 'RUNNING', NOW())
            """;

        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS)) {

            stmt.setString(1, modelId);
            stmt.setString(2, opTime);
            stmt.executeUpdate();

            try (ResultSet rs = stmt.getGeneratedKeys()) {
                if (rs.next()) {
                    Long taskId = rs.getLong(1);
                    log.info("[Task Log] Created RUNNING task: id={}, model={}, opTime={}", 
                            taskId, modelId, opTime);
                    return taskId;
                }
            }

        } catch (SQLException e) {
            log.error("[Task Log] Failed to create running task: model={}, opTime={}", 
                    modelId, opTime, e);
            throw new RuntimeException("Failed to create running task", e);
        }

        return null;
    }

    /**
     * 更新任务为SUCCESS
     */
    public void markSuccess(Long taskId, int computeCount, int storageCount) {
        String sql = """
            UPDATE metrics_task_log 
            SET status = 'SUCCESS', end_time = NOW(), compute_count = ?, storage_count = ?
            WHERE id = ?
            """;

        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {

            stmt.setInt(1, computeCount);
            stmt.setInt(2, storageCount);
            stmt.setLong(3, taskId);
            stmt.executeUpdate();

            log.info("[Task Log] Task marked SUCCESS: id={}, compute={}, storage={}", 
                    taskId, computeCount, storageCount);

        } catch (SQLException e) {
            log.error("[Task Log] Failed to mark success: id={}", taskId, e);
            throw new RuntimeException("Failed to mark task success", e);
        }
    }

    /**
     * 更新任务为FAILED
     */
    public void markFailed(Long taskId, String message) {
        String sql = """
            UPDATE metrics_task_log 
            SET status = 'FAILED', end_time = NOW(), message = ?
            WHERE id = ?
            """;

        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {

            stmt.setString(1, message);
            stmt.setLong(2, taskId);
            stmt.executeUpdate();

            log.warn("[Task Log] Task marked FAILED: id={}, message={}", taskId, message);

        } catch (SQLException e) {
            log.error("[Task Log] Failed to mark failed: id={}", taskId, e);
            throw new RuntimeException("Failed to mark task failed", e);
        }
    }

    /**
     * 检查是否有正在运行的任务（并发控制）
     */
    public boolean existsRunningTask(String modelId, String opTime) {
        String sql = """
            SELECT COUNT(*) FROM metrics_task_log 
            WHERE model_id = ? AND op_time = ? AND status = 'RUNNING'
            """;

        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {

            stmt.setString(1, modelId);
            stmt.setString(2, opTime);

            try (ResultSet rs = stmt.executeQuery()) {
                return rs.next() && rs.getInt(1) > 0;
            }

        } catch (SQLException e) {
            log.error("[Task Log] Failed to check running task: model={}, opTime={}", 
                    modelId, opTime, e);
            return false;
        }
    }

    /**
     * 获取最后一次成功的任务（用于重跑检测）
     */
    public Optional<TaskLog> findLastSuccessTask(String modelId, String opTime) {
        String sql = """
            SELECT * FROM metrics_task_log 
            WHERE model_id = ? AND op_time = ? AND status = 'SUCCESS'
            ORDER BY end_time DESC
            LIMIT 1
            """;

        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {

            stmt.setString(1, modelId);
            stmt.setString(2, opTime);

            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    return Optional.of(mapRow(rs));
                }
            }

        } catch (SQLException e) {
            log.error("[Task Log] Failed to find last success task: model={}, opTime={}", 
                    modelId, opTime, e);
        }

        return Optional.empty();
    }

    private TaskLog mapRow(ResultSet rs) throws SQLException {
        Timestamp startTs = rs.getTimestamp("start_time");
        Timestamp endTs = rs.getTimestamp("end_time");

        return new TaskLog(
                rs.getLong("id"),
                rs.getString("model_id"),
                rs.getString("op_time"),
                rs.getString("status"),
                startTs != null ? startTs.toLocalDateTime() : null,
                endTs != null ? endTs.toLocalDateTime() : null,
                rs.getString("message"),
                rs.getObject("compute_count") != null ? rs.getInt("compute_count") : null,
                rs.getObject("storage_count") != null ? rs.getInt("storage_count") : null
        );
    }
}
