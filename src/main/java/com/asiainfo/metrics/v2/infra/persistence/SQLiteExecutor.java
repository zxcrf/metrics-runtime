package com.asiainfo.metrics.v2.infra.persistence;

import com.asiainfo.metrics.v2.core.model.QueryContext;
import com.asiainfo.metrics.v2.infra.storage.StorageManager;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.*;

/**
 * SQLite执行器
 * 负责在内存数据库中执行SQL查询
 */
@ApplicationScoped
public class SQLiteExecutor {

    private static final Logger log = LoggerFactory.getLogger(SQLiteExecutor.class);

    @Inject
    StorageManager storageManager;

    /**
     * 执行SQL查询
     *
     * @param ctx 查询上下文
     * @param sql SQL查询字符串
     * @return 查询结果列表
     */
    public List<Map<String, Object>> executeQuery(QueryContext ctx, String sql) {
        if (sql == null || sql.isEmpty()) {
            return Collections.emptyList();
        }

        try (Connection conn = DriverManager.getConnection("jdbc:sqlite::memory:")) {
            Statement stmt = conn.createStatement();

            // 1. 附加所有需要的物理表数据库
            for (var req : ctx.getRequiredTables()) {
                String localPath = storageManager.downloadAndPrepare(req);
                String alias = ctx.getAlias(req.kpiId(), req.opTime());
                stmt.execute(String.format("ATTACH DATABASE '%s' AS %s", localPath, alias));
                log.debug("Attached database: {} as {}", localPath, alias);
            }

            // 2. 执行主查询
            log.debug("Executing SQL: {}", sql);
            ResultSet rs = stmt.executeQuery(sql);

            // 3. 转换结果集为Map列表
            return resultSetToList(rs);

        } catch (SQLException e) {
            log.error("SQLite execution failed", e);
            throw new RuntimeException("SQLite execution failed", e);
        }
    }

    /**
     * 将ResultSet转换为List<Map<String, Object>>
     *
     * @param rs ResultSet
     * @return 结果列表
     */
    private List<Map<String, Object>> resultSetToList(ResultSet rs) throws SQLException {
        ResultSetMetaData md = rs.getMetaData();
        int columns = md.getColumnCount();
        List<Map<String, Object>> list = new ArrayList<>();

        while (rs.next()) {
            Map<String, Object> row = new LinkedHashMap<>();
            for (int i = 1; i <= columns; ++i) {
                String columnName = md.getColumnLabel(i);
                Object value = rs.getObject(i);
                row.put(columnName, value);
            }
            list.add(row);
        }

        log.debug("Query returned {} rows", list.size());
        return list;
    }
}
