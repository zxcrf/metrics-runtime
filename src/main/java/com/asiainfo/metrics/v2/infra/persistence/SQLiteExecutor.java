package com.asiainfo.metrics.v2.infra.persistence;

import com.asiainfo.metrics.v2.core.model.PhysicalTableReq;
import com.asiainfo.metrics.v2.core.model.QueryContext;
import com.asiainfo.metrics.v2.infra.storage.StorageManager;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.*;
import java.util.function.Function;

/**
 * SQLite执行器 (Enhanced for Scalability)
 * 支持分批加载策略，突破 SQLite ATTACH 数量限制
 */
@ApplicationScoped
public class SQLiteExecutor {

    private static final Logger log = LoggerFactory.getLogger(SQLiteExecutor.class);

    // SQLite 默认 ATTACH 限制通常为 10，我们设为 5 以留出余量（如系统库、Temp库等）
    // 同时也控制单次 SQL 的复杂度
    private static final int BATCH_SIZE = 5;

    @Inject
    StorageManager storageManager;

    /**
     * 标准执行模式 (少量表)
     */
    public List<Map<String, Object>> executeQuery(QueryContext ctx, String sql) {
        if (sql == null || sql.isEmpty()) return Collections.emptyList();

        try (Connection conn = DriverManager.getConnection("jdbc:sqlite::memory:")) {
            Statement stmt = conn.createStatement();

            // 附加所有表 (假设数量 < 10)
            for (var req : ctx.getRequiredTables()) {
                attachDatabase(stmt, ctx, req);
            }

            return executeAndMap(stmt, sql);

        } catch (SQLException e) {
            log.error("SQLite standard execution failed", e);
            throw new RuntimeException("Query execution failed", e);
        }
    }

    /**
     * 暂存表执行模式 (大量表)
     * 分批将数据加载到内存暂存表，然后执行查询
     */
    public List<Map<String, Object>> executeWithStaging(
            QueryContext ctx,
            List<String> dims,
            Function<String, String> sqlProvider) {

        String stagingTable = "staging_data";

        try (Connection conn = DriverManager.getConnection("jdbc:sqlite::memory:")) {
            Statement stmt = conn.createStatement();
            conn.setAutoCommit(false); // 关闭事务加速插入

            // 1. 创建暂存表
            createStagingTable(stmt, stagingTable, dims);

            // 2. 分批加载数据
            List<PhysicalTableReq> allTables = new ArrayList<>(ctx.getRequiredTables());
            for (int i = 0; i < allTables.size(); i += BATCH_SIZE) {
                int end = Math.min(i + BATCH_SIZE, allTables.size());
                List<PhysicalTableReq> batch = allTables.subList(i, end);

                log.debug("Batch loading tables {} to {}", i, end);
                loadBatch(stmt, ctx, batch, stagingTable, dims);
            }

            conn.commit(); // 提交数据加载

            // 3. 生成并执行最终SQL
            String sql = sqlProvider.apply(stagingTable);
            return executeAndMap(stmt, sql);

        } catch (SQLException e) {
            log.error("SQLite staging execution failed", e);
            throw new RuntimeException("Staging query execution failed", e);
        }
    }

    private void attachDatabase(Statement stmt, QueryContext ctx, PhysicalTableReq req) throws SQLException {
        String localPath = storageManager.downloadAndPrepare(req);
        String alias = ctx.getAlias(req.kpiId(), req.opTime());
        // 确保 alias 合法
        String safeAlias = alias.replaceAll("[^a-zA-Z0-9_]", "");
        stmt.execute(String.format("ATTACH DATABASE '%s' AS %s", localPath, safeAlias));
    }

    private void detachDatabase(Statement stmt, String alias) throws SQLException {
        stmt.execute("DETACH DATABASE " + alias);
    }

    private void createStagingTable(Statement stmt, String tableName, List<String> dims) throws SQLException {
        StringBuilder ddl = new StringBuilder();
        ddl.append("CREATE TABLE ").append(tableName).append(" (");
        // 固定列
        ddl.append("kpi_id TEXT, op_time TEXT, kpi_val REAL");
        // 动态维度列
        for (String dim : dims) {
            ddl.append(", ").append(dim).append(" TEXT");
        }
        ddl.append(")");
        stmt.execute(ddl.toString());

        // 创建索引加速后续聚合
        if (!dims.isEmpty()) {
            String idxCols = String.join(", ", dims);
            stmt.execute(String.format("CREATE INDEX idx_%s_dims ON %s (%s)", tableName, tableName, idxCols));
        }
    }

    private void loadBatch(Statement stmt, QueryContext ctx, List<PhysicalTableReq> batch, String stagingTable, List<String> dims) throws SQLException {
        // A. Attach Batch
        for (PhysicalTableReq req : batch) {
            // 需要为这批生成临时的 Context Alias 或者直接生成
            // 这里我们需要确保 Context 里有别名，因为 downloadAndPrepare 需要
            // 简单起见，我们重新注册 Alias
            String alias = "batch_db_" + Math.abs(req.hashCode());
            ctx.registerAlias(req, alias);
            attachDatabase(stmt, ctx, req);
        }

        // B. Insert Data
        String dimFields = String.join(", ", dims);
        for (PhysicalTableReq req : batch) {
            String alias = ctx.getAlias(req.kpiId(), req.opTime());
            String sourceTable = req.toTableName();

            String insertSql = String.format(
                    "INSERT INTO %s (kpi_id, op_time, kpi_val, %s) " +
                            "SELECT '%s', '%s', kpi_val, %s FROM %s.%s",
                    stagingTable, dimFields,
                    req.kpiId(), req.opTime(), dimFields, alias, sourceTable
            );
            stmt.execute(insertSql);
        }

        // C. Detach Batch
        for (PhysicalTableReq req : batch) {
            String alias = ctx.getAlias(req.kpiId(), req.opTime());
            detachDatabase(stmt, alias);
        }
    }

    private List<Map<String, Object>> executeAndMap(Statement stmt, String sql) throws SQLException {
        log.debug("Executing SQL: \n{}", sql);
        long start = System.currentTimeMillis();
        ResultSet rs = stmt.executeQuery(sql);
        List<Map<String, Object>> results = resultSetToList(rs);
        log.debug("Query finished in {} ms, returned {} rows", (System.currentTimeMillis() - start), results.size());
        return results;
    }

    private List<Map<String, Object>> resultSetToList(ResultSet rs) throws SQLException {
        ResultSetMetaData md = rs.getMetaData();
        int columns = md.getColumnCount();
        List<Map<String, Object>> list = new ArrayList<>();
        while (rs.next()) {
            Map<String, Object> row = new LinkedHashMap<>();
            for (int i = 1; i <= columns; ++i) {
                row.put(md.getColumnLabel(i), rs.getObject(i));
            }
            list.add(row);
        }
        return list;
    }
}