package com.asiainfo.metrics.service;

import com.asiainfo.metrics.model.http.KpiQueryRequest;
import com.asiainfo.metrics.model.http.KpiQueryResult;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * KPI查询引擎统一接口
 * 所有引擎实现必须遵循以下约定：
 * 1. 查询结果必须按「维度组合+opTime」聚合
 * 2. 聚合结果包含opTime和kpiValues字段
 * 3. 缺失值使用NOT_EXISTS("--")占位符
 *
 * @author QvQ
 * @date 2025/11/11
 */
public interface KpiQueryEngine {
    static final String NOT_EXISTS = "--";

    /**
     * 异步查询KPI数据
     * @param request KPI查询请求
     * @return 包含聚合结果的异步未来
     */
//    CompletableFuture<KpiQueryResult> queryKpiDataAsync(KpiQueryRequest request);

    /**
     * 同步查询KPI数据
     * @param request KPI查询请求
     * @return 包含聚合结果的响应
     */
    KpiQueryResult queryKpiData(KpiQueryRequest request);

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
