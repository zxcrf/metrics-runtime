package com.asiainfo.metrics.repository;

import com.asiainfo.metrics.model.KpiDefinition;
import com.asiainfo.metrics.model.KpiModel;
import com.asiainfo.metrics.model.CompDimDef;
import com.asiainfo.metrics.model.DimDef;
import com.asiainfo.metrics.model.DimCodeConfig;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.agroal.api.AgroalDataSource;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.Objects;

/**
 * KPI元数据仓库
 * 负责从MySQL MetaDB查询KPI定义和依赖关系
 */
@ApplicationScoped
public class KpiMetadataRepository {

    private static final Logger log = LoggerFactory.getLogger(KpiMetadataRepository.class);

    @Inject
    @io.quarkus.agroal.DataSource("metadb")
    AgroalDataSource metadbDataSource;

    @Inject
    ObjectMapper objectMapper;

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
     * 从复杂表达式中提取所有KPI引用
     * 支持格式：${KPI_ID.timeModifier}，例如：
     * - ${KD2001.lastCycle}
     * - ${KD1002.current}
     * - ${KD1005.lastYear}
     *
     * @param timeModifier  current, lastCycle, lastYear
     * @param fullReference ${KD2001.lastCycle}
     * @return KPI引用列表，包含KPI ID和时间修饰符
     */
        public record KpiReference(String kpiId, String timeModifier, String fullReference) {

        @Override
            public String toString() {
                return fullReference;
            }
        }

    public List<KpiReference> extractKpiReferences(String expression) {
        List<KpiReference> references = new ArrayList<>();
        if (expression == null || expression.isEmpty()) {
            return references;
        }

        // 匹配 ${KPI_ID.timeModifier} 格式，时间修饰符可选
        // 支持格式：${KD1002} 或 ${KD1002.current} 或 ${KD1002.lastCycle} 等
        Pattern pattern = Pattern.compile("\\$\\{(K[DCYM]\\d{4})(?:\\.(current|lastCycle|lastYear))?\\}");
        Matcher matcher = pattern.matcher(expression);

        while (matcher.find()) {
            String kpiId = matcher.group(1);
            String timeModifier = matcher.group(2);
            String fullReference = matcher.group(0);

            // 如果用户省略了时间修饰符，默认为 "current"
            if (timeModifier == null || timeModifier.isEmpty()) {
                timeModifier = "current";
            }

            references.add(new KpiReference(kpiId, timeModifier, fullReference));
        }

        return references;
    }


    /**
     * 根据源表名称获取取数模型
     */
    public KpiModel getMetricsModelDef(String tableName) {
        if (tableName == null || tableName.isEmpty()) {
            return null;
        }

        String sql = """
        SELECT 
            model_id
            ,model_name
            ,model_type
            ,comp_dim_code
            ,model_ds_name
            ,model_sql
            ,t_state
            ,team_name
            ,create_time
            ,update_time
        FROM metrics_model_def
        WHERE model_sql like ?
        """;

        try (Connection conn = metadbDataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {

            stmt.setString(1, "%"+tableName+"%");

            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    return mapResultSetToMetricsModelDef(rs);
                }
            }

        } catch (SQLException e) {
            throw new RuntimeException("查询取数模型失败: " + tableName, e);
        }

        return null;
    }


    /**
     * 根据周期类型和维度编码获取所有需要计算的派生指标
     */
    public List<KpiDefinition> getExtendedKpisByCycleAndDim(String compDimCode, String cycleType) {
        if (compDimCode == null || compDimCode.isEmpty() || cycleType == null || cycleType.isEmpty()) {
            return new ArrayList<>();
        }

        String sql = "SELECT * FROM metrics_def WHERE kpi_type = 'EXTENDED' AND comp_dim_code = ? AND cycle_type = ?";

        List<KpiDefinition> result = new ArrayList<>();

        try (Connection conn = metadbDataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {

            stmt.setString(1, compDimCode);
            stmt.setString(2, cycleType.toLowerCase());

            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    result.add(mapResultSetToKpiDefinition(rs));
                }
            }

        } catch (SQLException e) {
            throw new RuntimeException("查询派生指标失败", e);
        }

        return result;
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

    /**
     * 映射ResultSet到MetricsModelDef
     */
    private KpiModel mapResultSetToMetricsModelDef(ResultSet rs) throws SQLException {
        return new KpiModel(
            rs.getString("model_id"),
            rs.getString("model_name"),
            rs.getString("model_type"),
            rs.getString("comp_dim_code"),
            rs.getString("model_ds_name"),
            rs.getString("model_sql"),
            rs.getString("t_state"),
            rs.getString("team_name"),
            rs.getString("create_time"),
            rs.getString("update_time")
        );
    }

    /**
     * 根据组合维度编码获取组合维度定义
     */
    public CompDimDef getCompDimDef(String compDimCode) {
        if (compDimCode == null || compDimCode.isEmpty()) {
            return null;
        }

        String sql = "SELECT * FROM metrics_comp_dim_def WHERE comp_dim_code = ?";

        try (Connection conn = metadbDataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {

            stmt.setString(1, compDimCode);

            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    return mapResultSetToCompDimDef(rs);
                }
            }

        } catch (SQLException e) {
            throw new RuntimeException("查询组合维度定义失败: " + compDimCode, e);
        }

        return null;
    }

    /**
     * 根据组合维度编码获取所有原子维度定义
     */
    public List<DimDef> getDimDefsByCompDim(String compDimCode) {
        if (compDimCode == null || compDimCode.isEmpty()) {
            return new ArrayList<>();
        }

        // 首先获取组合维度定义
        CompDimDef compDimDef = getCompDimDef(compDimCode);
        if (compDimDef == null || compDimDef.compDimConf() == null) {
            return new ArrayList<>();
        }

        try {
            // 解析compDimConf JSON，提取dimCode列表
            List<DimCodeConfig> dimCodeConfigs = objectMapper.readValue(
                compDimDef.compDimConf(),
                new TypeReference<List<DimCodeConfig>>() {}
            );

            if (dimCodeConfigs.isEmpty()) {
                return new ArrayList<>();
            }

            // 构建IN查询的占位符
            String dimCodes = dimCodeConfigs.stream()
                .map(DimCodeConfig::dimCode)
                .collect(Collectors.joining("','", "'", "'"));

            String sql = "SELECT * FROM metrics_dim_def WHERE dim_code IN (" + dimCodes + ")";

            List<DimDef> result = new ArrayList<>();

            try (Connection conn = metadbDataSource.getConnection();
                 PreparedStatement stmt = conn.prepareStatement(sql)) {

                try (ResultSet rs = stmt.executeQuery()) {
                    while (rs.next()) {
                        result.add(mapResultSetToDimDef(rs));
                    }
                }

            }

            return result;

        } catch (Exception e) {
            log.warn("解析维度配置失败，使用默认维度", e);
            // 解析失败时返回默认维度
            return getDefaultDimDefs();
        }
    }

    /**
     * 获取默认维度定义列表
     */
    private List<DimDef> getDefaultDimDefs() {
        // 返回默认的三个维度：D1001, D1002, D1003
        String sql = "SELECT * FROM metrics_dim_def WHERE dim_code IN ('D1001', 'D1002', 'D1003')";

        List<DimDef> result = new ArrayList<>();

        try (Connection conn = metadbDataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {

            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    result.add(mapResultSetToDimDef(rs));
                }
            }

        } catch (SQLException e) {
            throw new RuntimeException("查询默认维度定义失败", e);
        }

        return result;
    }

    /**
     * 根据组合维度编码获取数据库列名字符串（用于SQL中的GROUP BY）
     */
    public String getDimFieldsStringByCompDim(String compDimCode) {
        if (compDimCode == null || compDimCode.isEmpty()) {
            return "city_id, county_id, region_id"; // 默认值
        }

        try {
            // 动态获取维度定义
            List<DimDef> dimDefs = getDimDefsByCompDim(compDimCode);

            if (dimDefs.isEmpty()) {
                log.warn("未找到组合维度 {} 的维度定义，使用默认值", compDimCode);
                return "city_id, county_id, region_id";
            }

            // 将dimDefs转换为db_col_name列表
            return dimDefs.stream()
                .map(DimDef::dbColName)
                .filter(Objects::nonNull)
                .collect(Collectors.joining(", "));

        } catch (Exception e) {
            log.warn("获取维度字段失败，使用默认值", e);
            return "city_id, county_id, region_id";
        }
    }

    /**
     * 映射ResultSet到CompDimDef
     */
    private CompDimDef mapResultSetToCompDimDef(ResultSet rs) throws SQLException {
        return new CompDimDef(
            rs.getString("comp_dim_code"),
            rs.getString("comp_dim_name"),
            rs.getString("comp_dim_conf"),
            rs.getString("t_state"),
            rs.getString("team_name"),
            rs.getString("create_time"),
            rs.getString("update_time")
        );
    }

    /**
     * 映射ResultSet到DimDef
     */
    private DimDef mapResultSetToDimDef(ResultSet rs) throws SQLException {
        return new DimDef(
            rs.getString("dim_code"),
            rs.getString("dim_name"),
            rs.getString("dim_type"),
            rs.getString("dim_val_type"),
            rs.getString("dim_val_conf"),
            rs.getString("dim_desc"),
            rs.getString("db_col_name"),
            rs.getString("t_state"),
            rs.getString("create_time"),
            rs.getString("update_time")
        );
    }
}
