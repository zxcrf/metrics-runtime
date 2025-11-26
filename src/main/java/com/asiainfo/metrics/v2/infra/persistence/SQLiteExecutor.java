package com.asiainfo.metrics.v2.infra.persistence;

import com.asiainfo.metrics.v2.core.model.PhysicalTableReq;
import com.asiainfo.metrics.v2.core.model.QueryContext;
import com.asiainfo.metrics.v2.infra.storage.StorageManager;
import io.agroal.api.AgroalDataSource;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.quarkus.agroal.DataSource;
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
    MeterRegistry registry;

    // 1. 注入连接池
    @Inject
    @DataSource("sqlite")
    AgroalDataSource sqliteDataSource;

    public List<Map<String, Object>> executeQuery(QueryContext ctx, String sql) {
        if (sql == null || sql.isEmpty())
            return Collections.emptyList();

        // 2. 从连接池获取连接 (Reuse)
        try (Connection conn = sqliteDataSource.getConnection()) {
            Set<String> attachedAliases = new HashSet<>();
            try {
                Statement stmt = conn.createStatement();

                // Attach KPI Tables
                for (var req : ctx.getRequiredTables()) {
                    String alias = attachDatabase(stmt, ctx, req);
                    attachedAliases.add(alias);
                }

                // Attach Dim Tables
                List<String> dimAliases = attachDimensionTables(stmt, ctx);
                attachedAliases.addAll(dimAliases);

                // Execute
                return executeAndMap(stmt, sql);

            } finally {
                // 3. 关键：归还前必须清理现场 (Cleanup)
                // 如果不 DETACH，下次复用这个连接时会报错 "database ... is already in use"
                detachAll(conn, attachedAliases);
            }
        } catch (Exception e) {
            log.error("SQLite execution failed", e);
            throw new RuntimeException("Query execution failed", e);
        }
    }

    // Staging 模式同理适配
    public List<Map<String, Object>> executeWithStaging(QueryContext ctx, List<String> dims,
            Function<String, String> sqlProvider) {
        String stagingTable = "staging_data";
        try (Connection conn = sqliteDataSource.getConnection()) {
            Set<String> attachedAliases = new HashSet<>();
            Statement stmt = conn.createStatement();

            // 性能优化
            stmt.execute("PRAGMA journal_mode = OFF;");
            stmt.execute("PRAGMA synchronous = OFF;");

            conn.setAutoCommit(false);
            try {
                createStagingTable(stmt, stagingTable, dims);
                List<PhysicalTableReq> allTables = new ArrayList<>(ctx.getRequiredTables());

                for (int i = 0; i < allTables.size(); i += BATCH_SIZE) {
                    int end = Math.min(i + BATCH_SIZE, allTables.size());
                    List<PhysicalTableReq> batch = allTables.subList(i, end);

                    // Load Batch 会负责 Attach -> Insert -> Detach
                    // 所以这里只需要收集 staging 过程中产生的临时 alias (如果有残留)
                    loadBatch(stmt, ctx, batch, stagingTable, dims);
                }

                conn.commit();

                // 最后查询时的 Dimension Attach
                List<String> dimAliases = attachDimensionTables(stmt, ctx);
                attachedAliases.addAll(dimAliases);

                String sql = sqlProvider.apply(stagingTable);
                List<Map<String, Object>> result = executeAndMap(stmt, sql);

                // 清理 Staging 表 (因为连接是复用的，表会残留)
                stmt.execute("DROP TABLE IF EXISTS " + stagingTable);

                return result;
            } catch (Exception e) {
                conn.rollback();
                throw e;
            } finally {
                detachAll(conn, attachedAliases);
                // 确保 Staging 表被清理
                try {
                    stmt.execute("DROP TABLE IF EXISTS " + stagingTable);
                } catch (Exception ignored) {
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    // --- 辅助方法 ---

    private String attachDatabase(Statement stmt, QueryContext ctx, PhysicalTableReq req) throws Exception {
        String localPath = storageManager.downloadAndPrepare(req);
        String alias = ctx.getAlias(req.kpiId(), req.opTime());
        stmt.execute(String.format("ATTACH DATABASE '%s' AS %s", localPath, alias));
        return alias;
    }

    private List<String> attachDimensionTables(Statement stmt, QueryContext ctx) throws SQLException {
        List<String> aliases = new ArrayList<>();
        for (Map.Entry<String, String> entry : ctx.getDimensionTablePaths().entrySet()) {
            String alias = "dim_db_" + entry.getKey();
            stmt.execute(String.format("ATTACH DATABASE '%s' AS %s", entry.getValue(), alias));
            aliases.add(alias);
        }
        return aliases;
    }

    // 统一清理方法
    private void detachAll(Connection conn, Set<String> aliases) {
        if (aliases == null || aliases.isEmpty())
            return;
        try (Statement stmt = conn.createStatement()) {
            for (String alias : aliases) {
                try {
                    stmt.execute("DETACH DATABASE " + alias);
                } catch (SQLException e) {
                    // 忽略 DETACH 失败，可能是已经 DETACH 了
                    log.warn("Failed to detach database: {}", alias, e);
                }
            }
        } catch (SQLException e) {
            log.error("Failed to create statement for detach", e);
        }
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
        Set<String> batchAliases = new HashSet<>();
        for (PhysicalTableReq req : batch) {
            batchAliases.add(attachDatabase(stmt, ctx, req));
        }

        String selectDims = dims.isEmpty() ? "" : ", " + String.join(", ", dims);
        String insertDims = dims.isEmpty() ? "" : ", " + String.join(", ", dims);
        for (PhysicalTableReq req : batch) {
            String alias = ctx.getAlias(req.kpiId(), req.opTime());
            String insertSql = String.format(
                    "INSERT INTO %s (kpi_id, op_time, kpi_val%s) SELECT '%s', '%s', kpi_val%s FROM %s.%s",
                    stagingTable, insertDims, req.kpiId(), req.opTime(), selectDims, alias, req.toTableName());
            stmt.execute(insertSql);
        }

        // Batch 结束立即 Detach
        for (String alias : batchAliases) {
            stmt.execute("DETACH DATABASE " + alias);
        }
    }

    private List<Map<String, Object>> executeAndMap(Statement stmt, String sql) throws Exception {
        if (sql == null || sql.isEmpty())
            return Collections.emptyList();
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