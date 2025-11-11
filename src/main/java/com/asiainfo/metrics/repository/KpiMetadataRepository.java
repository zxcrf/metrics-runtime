package com.asiainfo.metrics.repository;

import com.asiainfo.metrics.model.KpiDefinition;
import io.agroal.api.AgroalDataSource;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * KPI元数据仓库
 * 负责从MySQL MetaDB查询KPI定义和依赖关系
 */
@ApplicationScoped
public class KpiMetadataRepository {

    @Inject
    @io.quarkus.agroal.DataSource("metadb")
    AgroalDataSource metadbDataSource;

    /**
     * 批量查询KPI定义
     */
//    @CacheName(cacheName = "kpi-metadata")
    public Map<String, KpiDefinition> batchGetKpiDefinitions(List<String> kpiIds) {
        if (kpiIds == null || kpiIds.isEmpty()) {
            return new HashMap<>();
        }

        String sql = "SELECT * " +
                     "FROM metrics_def WHERE kpi_id IN (" +
                     kpiIds.stream().map(id -> "?").collect(Collectors.joining(",")) + ")";

        Map<String, KpiDefinition> result = new HashMap<>();

        try (Connection conn = metadbDataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {

            for (int i = 0; i < kpiIds.size(); i++) {
                stmt.setString(i + 1, kpiIds.get(i));
            }

            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    KpiDefinition def = mapResultSetToKpiDefinition(rs);
                    result.put(def.kpiId(), def);
                }
            }

        } catch (SQLException e) {
            throw new RuntimeException("查询KPI定义失败", e);
        }

        return result;
    }

    /**
     * 获取KPI定义
     */
//    @CacheResult(cacheName = "kpi-metadata")
    public KpiDefinition getKpiDefinition(String kpiId) {
        if (kpiId == null || kpiId.isEmpty()) {
            return null;
        }

        String sql = "SELECT * " +
                     "FROM metrics_def WHERE kpi_id = ?";

        try (Connection conn = metadbDataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {

            stmt.setString(1, kpiId);

            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    return mapResultSetToKpiDefinition(rs);
                }
            }

        } catch (SQLException e) {
            throw new RuntimeException("查询KPI定义失败: " + kpiId, e);
        }

        return null;
    }

    /**
     * 映射ResultSet到KpiDefinition
     */
    private KpiDefinition mapResultSetToKpiDefinition(ResultSet rs) throws SQLException {
        return new KpiDefinition(
            rs.getString("kpi_id"),
            rs.getString("kpi_name"),
            rs.getString("kpi_type"),
            rs.getString("comp_dim_code"),
            rs.getString("cycle_type"),
            rs.getString("topic_id"),
            rs.getString("team_name"),
            rs.getString("kpi_expr"),
            rs.getString("create_time"),
            rs.getString("update_time")
        );
    }
}
