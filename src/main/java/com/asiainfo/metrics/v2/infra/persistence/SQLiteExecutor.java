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

    public List<Map<String, Object>> executeQuery(QueryContext ctx, String sql) {
        if (sql == null || sql.isEmpty())
            return Collections.emptyList();

        try (Connection conn = DriverManager.getConnection("jdbc:sqlite::memory:")) {
            Statement stmt = conn.createStatement();

            // 1. Attach KPI Tables
            for (var req : ctx.getRequiredTables()) {
                attachDatabase(stmt, ctx, req);
            }

            // 2. Attach Dimension Tables (新增)
            attachDimensionTables(stmt, ctx);

            return executeAndMap(stmt, sql);

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

        try (Connection conn = DriverManager.getConnection("jdbc:sqlite::memory:")) {
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

                // 在执行最终查询之前，Attach 维度表
                // 因为 Staging 模式下，最后的 SQL 依然会 JOIN 维度表
                attachDimensionTables(stmt, ctx);

                String sql = sqlProvider.apply(stagingTable);
                return executeAndMap(stmt, sql);
            } catch (Exception e) {
                conn.rollback();
                throw e;
            }

        } catch (Exception e) {
            log.error("Staging execution failed", e);
            throw new RuntimeException("Staging query execution failed", e);
        }
    }

    private void attachDatabase(Statement stmt, QueryContext ctx, PhysicalTableReq req) throws Exception {
        String localPath = storageManager.downloadAndPrepare(req);
        String alias = ctx.getAlias(req.kpiId(), req.opTime());
        stmt.execute(String.format("ATTACH DATABASE '%s' AS %s", localPath, alias));
    }

    // 新增：Attach 维度表
    private void attachDimensionTables(Statement stmt, QueryContext ctx) throws SQLException {
        for (Map.Entry<String, String> entry : ctx.getDimensionTablePaths().entrySet()) {
            String compDimCode = entry.getKey();
            String path = entry.getValue();
            // 使用特定的 Alias，虽然 SQLite 允许直接用表名访问（只要不冲突），
            // 但为了避免冲突，我们可以给 DB 一个别名，表名保持不变。
            // 这里的 Alias 主要是为了挂载 DB。
            String alias = "dim_db_" + compDimCode;
            stmt.execute(String.format("ATTACH DATABASE '%s' AS %s", path, alias));
        }
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