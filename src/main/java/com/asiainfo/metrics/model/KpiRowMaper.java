package com.asiainfo.metrics.model;

import com.asiainfo.metrics.model.db.KpiDefinition;
import com.asiainfo.metrics.model.db.KpiModel;

import java.sql.ResultSet;
import java.sql.SQLException;

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
}
