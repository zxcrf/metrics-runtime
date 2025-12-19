package com.asiainfo.metrics.v2.application.etl;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.inject.Named;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * ETL日志查询服务
 * 提供日志查询、统计和历史记录功能
 */
@ApplicationScoped
public class ETLLogQueryService {

    private static final Logger log = LoggerFactory.getLogger(ETLLogQueryService.class);

    @Inject
    @Named("metadb")
    DataSource metaDataSource;

    /**
     * 分页查询ETL日志
     */
    public QueryResult queryLogs(
            String kpiModelId, String opTime, String status, String executionType,
            String startDate, String endDate, int page, int pageSize) throws SQLException {

        StringBuilder sql = new StringBuilder(
                "SELECT * FROM kpi_etl_execution_log WHERE 1=1");
        List<Object> params = new ArrayList<>();

        if (kpiModelId != null && !kpiModelId.isEmpty()) {
            sql.append(" AND kpi_model_id = ?");
            params.add(kpiModelId);
        }
        if (opTime != null && !opTime.isEmpty()) {
            sql.append(" AND op_time = ?");
            params.add(opTime);
        }
        if (status != null && !status.isEmpty()) {
            sql.append(" AND status = ?");
            params.add(status);
        }
        if (executionType != null && !executionType.isEmpty()) {
            sql.append(" AND execution_type = ?");
            params.add(executionType);
        }
        if (startDate != null && !startDate.isEmpty()) {
            sql.append(" AND started_at >= ?");
            params.add(startDate + " 00:00:00");
        }
        if (endDate != null && !endDate.isEmpty()) {
            sql.append(" AND started_at <= ?");
            params.add(endDate + " 23:59:59");
        }

        // 获取总数
        String countSql = sql.toString().replace("*", "COUNT(*)");
        int total = 0;
        try (Connection conn = metaDataSource.getConnection();
                PreparedStatement stmt = conn.prepareStatement(countSql)) {
            setParameters(stmt, params);
            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    total = rs.getInt(1);
                }
            }
        }

        // 分页查询
        sql.append(" ORDER BY started_at DESC LIMIT ? OFFSET ?");
        params.add(pageSize);
        params.add((page - 1) * pageSize);

        List<Map<String, Object>> data = new ArrayList<>();
        try (Connection conn = metaDataSource.getConnection();
                PreparedStatement stmt = conn.prepareStatement(sql.toString())) {
            setParameters(stmt, params);
            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    data.add(mapRow(rs));
                }
            }
        }

        return new QueryResult(total, page, pageSize, data);
    }

    /**
     * 获取日志详情
     */
    public Map<String, Object> getLogDetail(Long id) throws SQLException {
        String sql = "SELECT * FROM kpi_etl_execution_log WHERE id = ?";

        try (Connection conn = metaDataSource.getConnection();
                PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setLong(1, id);
            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    return mapRow(rs);
                }
            }
        }
        return null;
    }

    /**
     * 获取最近执行记录
     */
    public List<Map<String, Object>> getRecentExecutions(String kpiModelId, int limit) throws SQLException {
        String sql = "SELECT * FROM kpi_etl_execution_log WHERE kpi_model_id = ? " +
                "ORDER BY started_at DESC LIMIT ?";

        List<Map<String, Object>> result = new ArrayList<>();
        try (Connection conn = metaDataSource.getConnection();
                PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setString(1, kpiModelId);
            stmt.setInt(2, limit);
            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    result.add(mapRow(rs));
                }
            }
        }
        return result;
    }

    /**
     * 获取统计信息
     */
    public Map<String, Object> getStats(String startDate, String endDate) throws SQLException {
        StringBuilder sql = new StringBuilder(
                "SELECT " +
                        "COUNT(*) as total_executions, " +
                        "SUM(CASE WHEN status='SUCCESS' THEN 1 ELSE 0 END) as success_count, " +
                        "SUM(CASE WHEN status='FAILED' THEN 1 ELSE 0 END) as failed_count, " +
                        "SUM(CASE WHEN execution_type='REDO' THEN 1 ELSE 0 END) as redo_count, " +
                        "AVG(execution_time_ms) as avg_execution_time_ms " +
                        "FROM kpi_etl_execution_log WHERE 1=1");

        List<Object> params = new ArrayList<>();
        if (startDate != null && !startDate.isEmpty()) {
            sql.append(" AND started_at >= ?");
            params.add(startDate + " 00:00:00");
        }
        if (endDate != null && !endDate.isEmpty()) {
            sql.append(" AND started_at <= ?");
            params.add(endDate + " 23:59:59");
        }

        Map<String, Object> stats = new HashMap<>();
        try (Connection conn = metaDataSource.getConnection();
                PreparedStatement stmt = conn.prepareStatement(sql.toString())) {
            setParameters(stmt, params);
            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    stats.put("totalExecutions", rs.getInt("total_executions"));
                    stats.put("successCount", rs.getInt("success_count"));
                    stats.put("failedCount", rs.getInt("failed_count"));
                    stats.put("redoCount", rs.getInt("redo_count"));
                    stats.put("avgExecutionTimeMs", rs.getLong("avg_execution_time_ms"));
                }
            }
        }

        return stats;
    }

    private void setParameters(PreparedStatement stmt, List<Object> params) throws SQLException {
        for (int i = 0; i < params.size(); i++) {
            stmt.setObject(i + 1, params.get(i));
        }
    }

    private Map<String, Object> mapRow(ResultSet rs) throws SQLException {
        Map<String, Object> row = new HashMap<>();
        row.put("id", rs.getLong("id"));
        row.put("kpiModelId", rs.getString("kpi_model_id"));
        row.put("opTime", rs.getString("op_time"));
        row.put("executionType", rs.getString("execution_type"));
        row.put("status", rs.getString("status"));
        row.put("recordCount", rs.getInt("record_count"));
        row.put("errorMessage", rs.getString("error_message"));
        row.put("executionTimeMs", rs.getLong("execution_time_ms"));
        row.put("startedAt", rs.getTimestamp("started_at"));
        row.put("completedAt", rs.getTimestamp("completed_at"));
        row.put("operator", rs.getString("operator"));
        row.put("remark", rs.getString("remark"));
        return row;
    }

    /**
     * 查询结果
     */
    public record QueryResult(
            int total,
            int page,
            int pageSize,
            List<Map<String, Object>> data) {
    }
}
