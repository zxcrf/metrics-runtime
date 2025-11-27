package com.asiainfo.metrics.v2.infra.persistence;

import com.asiainfo.metrics.v2.core.model.PhysicalTableReq;
import com.asiainfo.metrics.v2.core.model.QueryContext;
import com.asiainfo.metrics.v2.infra.storage.StorageManager;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.*;
import java.util.function.Function;

@ApplicationScoped
public class SQLiteExecutor {

    private static final Logger log = LoggerFactory.getLogger(SQLiteExecutor.class);
    private static final int BATCH_SIZE = 10;

    @Inject
    StorageManager storageManager;
    @Inject
    MeterRegistry registry; // 注入

    @Inject
    @io.quarkus.agroal.DataSource("sqlite")
    javax.sql.DataSource sqliteDs;

    public List<Map<String, Object>> executeQuery(QueryContext ctx, String sql) {
        if (sql == null || sql.isEmpty())
            return Collections.emptyList();

        List<String> kpiAliases = new ArrayList<>();
        try (Connection conn = sqliteDs.getConnection()) {
            Statement stmt = conn.createStatement();
            // 确保性能配置
            stmt.execute("PRAGMA journal_mode = OFF;");
            stmt.execute("PRAGMA synchronous = OFF;");

            try {
                // 获取当前挂载的数据库
                Map<String, String> attachedDbs = getAttachedDatabases(stmt);

                // 1. Attach KPI Tables (Always attach, always detach)
                for (var req : ctx.getRequiredTables()) {
                    String alias = attachDatabase(stmt, ctx, req);
                    kpiAliases.add(alias);
                }

                // 2. Attach Dimension Tables (Sticky)
                attachDimensionTables(stmt, ctx, attachedDbs);

                return executeAndMap(stmt, sql);
            } finally {
                // 清理：只 Detach KPI 数据库
                for (String alias : kpiAliases) {
                    try {
                        detachDatabase(stmt, alias);
                    } catch (Exception e) {
                        log.warn("Failed to detach database: {}", alias, e);
                    }
                }
            }

        } catch (SQLException e) {
            log.error("SQLite execution failed", e);
            throw new RuntimeException("Query execution failed", e);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public List<Map<String, Object>> executeWithStaging(
            QueryContext ctx,
            List<String> dims,
            Function<String, String> sqlProvider) {

        String stagingTable = "staging_data";

        try (Connection conn = sqliteDs.getConnection()) {
            Statement stmt = conn.createStatement();
            stmt.execute("PRAGMA journal_mode = OFF;");
            stmt.execute("PRAGMA synchronous = OFF;");

            conn.setAutoCommit(false);

            try {
                createStagingTable(stmt, stagingTable, dims);

                List<PhysicalTableReq> allTables = new ArrayList<>(ctx.getRequiredTables());
                for (int i = 0; i < allTables.size(); i += BATCH_SIZE) {
                    int end = Math.min(i + BATCH_SIZE, allTables.size());
                    List<PhysicalTableReq> batch = allTables.subList(i, end);
                    loadBatch(stmt, ctx, batch, stagingTable, dims);
                }

                conn.commit();

                conn.commit();

                // 在执行最终查询之前，Attach 维度表
                Map<String, String> attachedDbs = getAttachedDatabases(stmt);
                attachDimensionTables(stmt, ctx, attachedDbs);

                try {
                    String sql = sqlProvider.apply(stagingTable);
                    return executeAndMap(stmt, sql);
                } finally {
                    // Staging 模式下，维度表也保持 Sticky，不需要卸载
                    // 清理 Staging 表
                    stmt.execute("DROP TABLE IF EXISTS " + stagingTable);
                }
            } catch (Exception e) {
                conn.rollback();
                throw e;
            }

        } catch (Exception e) {
            log.error("Staging execution failed", e);
            throw new RuntimeException("Staging query execution failed", e);
        }
    }

    private Map<String, String> getAttachedDatabases(Statement stmt) throws SQLException {
        Map<String, String> attached = new HashMap<>();
        try (ResultSet rs = stmt.executeQuery("PRAGMA database_list")) {
            while (rs.next()) {
                attached.put(rs.getString("name"), rs.getString("file"));
            }
        }
        return attached;
    }

    private String attachDatabase(Statement stmt, QueryContext ctx, PhysicalTableReq req) throws Exception {
        String localPath = storageManager.downloadAndPrepare(req);
        String alias = ctx.getAlias(req.kpiId(), req.opTime());
        stmt.execute(String.format("ATTACH DATABASE '%s' AS %s", localPath, alias));
        return alias;
    }

    // 优化：Smart Attach 维度表 (Sticky Dimensions)
    private List<String> attachDimensionTables(Statement stmt, QueryContext ctx, Map<String, String> attachedDbs)
            throws SQLException {
        List<String> attachedAliases = new ArrayList<>();
        for (Map.Entry<String, String> entry : ctx.getDimensionTablePaths().entrySet()) {
            String compDimCode = entry.getKey();
            String path = entry.getValue();
            String alias = "dim_db_" + compDimCode;

            // 如果已经挂载且路径一致，则跳过
            if (attachedDbs.containsKey(alias)) {
                String currentPath = attachedDbs.get(alias);
                if (currentPath.equals(path)) {
                    continue; // Sticky!
                } else {
                    // 路径变了（版本更新），先卸载
                    stmt.execute("DETACH DATABASE " + alias);
                }
            }

            stmt.execute(String.format("ATTACH DATABASE '%s' AS %s", path, alias));
            // 注意：这里我们不把 alias 加入到 returned list 中，
            // 因为我们不想在 finally 块中卸载它！我们希望它 sticky。
            // 除非我们需要在本次请求结束时强制卸载（比如 staging 模式可能需要？）
            // 对于 executeQuery，我们希望保留。
        }
        return attachedAliases;
    }

    // ... detachDatabase, createStagingTable, loadBatch, executeAndMap,
    // resultSetToList 保持不变 ...
    // 注意：loadBatch 中不需要 attachDimensionTables，因为 loadBatch 只负责把 KPI 数据搬运到 Staging 表
    // 维度表只在最后做 JOIN 时需要

    private void detachDatabase(Statement stmt, String alias) throws SQLException {
        stmt.execute("DETACH DATABASE " + alias);
    }

    private void createStagingTable(Statement stmt, String tableName, List<String> dims) throws SQLException {
        StringBuilder ddl = new StringBuilder();
        ddl.append("CREATE TABLE ").append(tableName).append(" (");
        ddl.append("kpi_id TEXT, op_time TEXT, kpi_val REAL");
        for (String dim : dims) {
            ddl.append(", ").append(dim).append(" TEXT");
        }
        ddl.append(")");
        stmt.execute(ddl.toString());
    }

    @Inject
    MetadataRepository metadataRepo;

    // ... (existing code)

    private void loadBatch(Statement stmt, QueryContext ctx, List<PhysicalTableReq> batch, String stagingTable,
            List<String> dims) throws Exception {
        for (PhysicalTableReq req : batch) {
            attachDatabase(stmt, ctx, req);
        }

        String dimFields = String.join(", ", dims);
        String insertDims = dims.isEmpty() ? "" : ", " + dimFields;

        for (PhysicalTableReq req : batch) {
            String alias = ctx.getAlias(req.kpiId(), req.opTime());
            String sourceTable = req.toTableName();

            // Smart Select Logic
            Set<String> tableActualDims = metadataRepo.getDimCols(req.compDimCode());
            StringBuilder smartSelect = new StringBuilder();

            for (String dim : dims) {
                if (tableActualDims.contains(dim)) {
                    smartSelect.append(", ").append(dim);
                } else {
                    smartSelect.append(", NULL as ").append(dim);
                }
            }

            String insertSql = String.format(
                    "INSERT INTO %s (kpi_id, op_time, kpi_val%s) SELECT '%s', '%s', kpi_val%s FROM %s.%s",
                    stagingTable, insertDims,
                    req.kpiId(), req.opTime(), smartSelect.toString(),
                    alias, sourceTable);
            stmt.execute(insertSql);
        }

        // Commit transaction to release locks on attached databases before detaching
        stmt.getConnection().commit();

        for (PhysicalTableReq req : batch) {
            String alias = ctx.getAlias(req.kpiId(), req.opTime());
            detachDatabase(stmt, alias);
        }
    }

    private List<Map<String, Object>> executeAndMap(Statement stmt, String sql) throws Exception {
        if (sql == null || sql.isEmpty())
            return Collections.emptyList();

        // 【埋点】记录 SQL 执行耗时
        return Timer.builder("metrics.sqlite.query.time")
                .description("SQLite query execution time")
                .register(registry)
                .recordCallable(() -> {
                    long start = System.currentTimeMillis();
                    try (ResultSet rs = stmt.executeQuery(sql)) {
                        List<Map<String, Object>> results = resultSetToList(rs);
                        log.debug("Executed in {} ms, rows: {}", System.currentTimeMillis() - start, results.size());
                        return results;
                    }
                });
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