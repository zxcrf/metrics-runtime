package com.asiainfo.metrics.v2.infra.persistence;

import com.asiainfo.metrics.v2.core.model.MetricDefinition;
import com.asiainfo.metrics.v2.core.model.MetricType;
import jakarta.enterprise.context.ApplicationScoped;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

/**
 * 元数据仓库
 * 负责从数据库查询KPI指标的元数据定义
 */
@ApplicationScoped
public class MetadataRepository {

    private static final Logger log = LoggerFactory.getLogger(MetadataRepository.class);

    // 缓存KPI定义，避免重复查询
    private final Map<String, MetricDefinition> cache = new HashMap<>();

    /**
     * 根据KPI ID查找指标定义
     *
     * @param kpiId KPI ID，如 KD1002
     * @return 指标定义
     */
    public MetricDefinition findById(String kpiId) {
        // 先检查缓存
        if (cache.containsKey(kpiId)) {
            return cache.get(kpiId);
        }

        try {
            // 从元数据库查询真实定义
            MetricDefinition definition = queryFromMetadataDb(kpiId);
            if (definition != null) {
                cache.put(kpiId, definition);
                log.debug("Loaded KPI definition: {}", kpiId);
                return definition;
            }
        } catch (Exception e) {
            log.warn("Failed to query KPI definition from metadata DB: {}", kpiId, e);
        }

        // 如果查询失败，创建默认的物理指标定义
        MetricDefinition defaultDef = MetricDefinition.physical(kpiId, "sum");
        cache.put(kpiId, defaultDef);
        log.debug("Using default definition for KPI: {}", kpiId);
        return defaultDef;
    }

    /**
     * 从元数据库查询KPI定义
     * 在实际生产环境中，这里会连接MySQL等元数据库
     *
     * @param kpiId KPI ID
     * @return 指标定义，如果未找到则返回null
     */
    private MetricDefinition queryFromMetadataDb(String kpiId) {
        try {
            // TODO: 实现真实的元数据库查询逻辑
            // 这里可以连接MySQL查询metrics_def表

            // 示例伪代码：
            // Connection conn = dataSource.getConnection();
            // String sql = "SELECT kpi_id, kpi_name, kpi_type, compute_method, kpi_expr, agg_func FROM metrics_def WHERE kpi_id = ?";
            // PreparedStatement stmt = conn.prepareStatement(sql);
            // stmt.setString(1, kpiId);
            // ResultSet rs = stmt.executeQuery();
            // if (rs.next()) {
            //     return mapResultSetToMetricDefinition(rs);
            // }

            return null;
        } catch (Exception e) {
            log.error("Failed to query metadata DB for KPI: {}", kpiId, e);
            return null;
        }
    }

    /**
     * 将ResultSet映射为MetricDefinition
     *
     * @param rs ResultSet
     * @return MetricDefinition
     */
    private MetricDefinition mapResultSetToMetricDefinition(ResultSet rs) throws Exception {
        String kpiId = rs.getString("kpi_id");
        String kpiType = rs.getString("kpi_type");
        String computeMethod = rs.getString("compute_method");
        String kpiExpr = rs.getString("kpi_expr");
        String aggFunc = rs.getString("agg_func");

        MetricType type;
        String expression;

        if ("extended".equalsIgnoreCase(kpiType)) {
            type = MetricType.PHYSICAL;
            expression = "${" + kpiId + ".current}";
        } else if ("composite".equalsIgnoreCase(kpiType) && "expr".equalsIgnoreCase(computeMethod)) {
            type = MetricType.COMPOSITE;
            expression = kpiExpr;
        } else {
            type = MetricType.PHYSICAL;
            expression = "${" + kpiId + ".current}";
        }

        return new MetricDefinition(kpiId, expression, type, aggFunc != null ? aggFunc : "sum");
    }

    /**
     * 清空缓存
     */
    public void clearCache() {
        cache.clear();
        log.debug("Metadata repository cache cleared");
    }
}
