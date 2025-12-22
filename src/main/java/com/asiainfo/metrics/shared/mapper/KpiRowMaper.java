package com.asiainfo.metrics.shared.mapper;

import com.asiainfo.metrics.domain.model.KpiDefinition;
import com.asiainfo.metrics.domain.model.KpiModel;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 *
 *
 * @author QvQ
 * @date 2025/11/14
 */
public class KpiRowMaper {

    /**
     * 映射ResultSet到KpiDefinition
     */
    public static KpiDefinition kpiDefinition(ResultSet rs) throws SQLException {
        return new KpiDefinition(
                rs.getString("kpi_id"),
                rs.getString("kpi_name"),
                rs.getString("kpi_type"),
                rs.getString("comp_dim_code"),
                rs.getString("cycle_type"),
                rs.getString("topic_id"),
                rs.getString("team_name"),
                rs.getString("kpi_expr"),
                rs.getString("compute_method"),
                rs.getString("agg_func"),
                rs.getString("create_time"),
                rs.getString("update_time")
        );
    }

    /**
     * 映射ResultSet到MetricsModelDef
     */
    public static KpiModel kpiModel(ResultSet rs) throws SQLException {
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


    public static void sqlRowMapping(List<Map<String, Object>> resultList, ResultSet rs) throws SQLException {
        ResultSetMetaData metaData = rs.getMetaData();
        int columnCount = metaData.getColumnCount();

        while (rs.next()) {
            Map<String, Object> row = new LinkedHashMap<>();
            for (int i = 1; i <= columnCount; i++) {
                String columnName = metaData.getColumnLabel(i);
                Object value = rs.getObject(i);
                row.put(columnName, value);
            }
            resultList.add(row);
        }
    }
}
