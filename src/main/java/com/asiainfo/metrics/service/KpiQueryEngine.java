package com.asiainfo.metrics.service;

import com.asiainfo.metrics.model.KpiQueryRequest;
import com.asiainfo.metrics.model.KpiQueryResult;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 *
 *
 * @author QvQ
 * @date 2025/11/11
 */
public interface KpiQueryEngine {
    static final String NOT_EXISTS = "--";

    CompletableFuture<KpiQueryResult> queryKpiDataAsync(KpiQueryRequest request);
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
