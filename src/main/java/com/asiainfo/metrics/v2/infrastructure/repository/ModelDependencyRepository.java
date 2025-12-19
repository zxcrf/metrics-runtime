package com.asiainfo.metrics.v2.infrastructure.repository;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.inject.Named;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * 模型依赖关系 Repository
 * 管理 metrics_model_dependency 表
 */
@ApplicationScoped
public class ModelDependencyRepository {

    private static final Logger log = LoggerFactory.getLogger(ModelDependencyRepository.class);

    @Inject
    @Named("metadb")
    DataSource dataSource;

    /**
     * 根据来源表名查找依赖此表的所有模型ID
     * 
     * @param tableName 来源表名
     * @return 依赖此表的模型ID列表
     */
    public List<String> findModelsByDependencyTableName(String tableName) {
        String sql = "SELECT model_id FROM metrics_model_dependency WHERE dependency_table_name = ?";
        List<String> modelIds = new ArrayList<>();

        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {

            stmt.setString(1, tableName);

            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    modelIds.add(rs.getString("model_id"));
                }
            }

            log.debug("[Dependency] Found {} models depending on table: {}", 
                    modelIds.size(), tableName);

        } catch (SQLException e) {
            log.error("[Dependency] Failed to find models by dependency table: {}", tableName, e);
            throw new RuntimeException("Failed to find models by dependency table", e);
        }

        return modelIds;
    }

    /**
     * 获取模型的所有依赖表
     * 
     * @param modelId 模型ID
     * @return 依赖表名列表
     */
    public List<String> getDependenciesForModel(String modelId) {
        String sql = "SELECT dependency_table_name FROM metrics_model_dependency WHERE model_id = ?";
        List<String> dependencies = new ArrayList<>();

        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {

            stmt.setString(1, modelId);

            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    dependencies.add(rs.getString("dependency_table_name"));
                }
            }

            log.debug("[Dependency] Model {} depends on {} tables", modelId, dependencies.size());

        } catch (SQLException e) {
            log.error("[Dependency] Failed to get dependencies for model: {}", modelId, e);
            throw new RuntimeException("Failed to get dependencies for model", e);
        }

        return dependencies;
    }

    /**
     * 添加模型依赖关系
     */
    public void addDependency(String modelId, String dependencyTableName) {
        String sql = """
            INSERT IGNORE INTO metrics_model_dependency (model_id, dependency_table_name)
            VALUES (?, ?)
            """;

        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {

            stmt.setString(1, modelId);
            stmt.setString(2, dependencyTableName);
            stmt.executeUpdate();

        } catch (SQLException e) {
            log.error("[Dependency] Failed to add dependency: model={}, table={}", 
                    modelId, dependencyTableName, e);
            throw new RuntimeException("Failed to add dependency", e);
        }
    }
}
