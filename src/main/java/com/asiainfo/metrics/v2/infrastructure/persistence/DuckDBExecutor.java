package com.asiainfo.metrics.v2.infrastructure.persistence;

import com.asiainfo.metrics.v2.domain.model.PhysicalTableReq;
import com.asiainfo.metrics.v2.domain.model.QueryContext;
import com.asiainfo.metrics.v2.domain.model.SqlRequest;
import com.asiainfo.metrics.v2.infrastructure.storage.StorageManager;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.infrastructure.Infrastructure;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.*;
import java.util.*;
import java.util.function.Function;

@ApplicationScoped
public class DuckDBExecutor {

    private static final Logger log = LoggerFactory.getLogger(DuckDBExecutor.class);
    private static final int BATCH_SIZE = 10;

    @Inject
    StorageManager storageManager;
    @Inject
    MeterRegistry registry;

    @Inject
    @io.quarkus.agroal.DataSource("duckdb")
    javax.sql.DataSource duckdbDs;

    @Inject
    MetadataRepository metadataRepo;

    // Initialize DuckDB extensions if needed
    // Note: In a real production environment with restricted internet, extensions
    // should be pre-bundled.
    // For this environment, we attempt to load it. If it fails, we might need
    // another strategy.
    private void initExtensions(Connection conn) throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            // Attempt to load sqlite extension.
            // If this fails due to network, we might need to rely on native DuckDB files or
            // pre-installed extension.
            // For now, we assume it might work or we catch the error.
            // stmt.execute("INSTALL sqlite;"); // Commented out to avoid network call if
            // not needed/allowed
            // stmt.execute("LOAD sqlite;");
        } catch (Exception e) {
            log.warn("Failed to load sqlite extension. Querying SQLite files might fail if not natively supported.", e);
        }
    }

    public List<Map<String, Object>> executeQuery(QueryContext ctx, String sql) {
        try (Connection conn = duckdbDs.getConnection();
                Statement stmt = conn.createStatement()) {
            return executeAndMap(stmt, sql);
        } catch (SQLException e) {
            log.error("DuckDB Query Failed: {}", sql, e);
            throw new RuntimeException("DuckDB Query Failed", e);
        } catch (Exception e) {
            log.error("DuckDB Execution Failed", e);
            throw new RuntimeException(e);
        }
    }

    // Unused methods removed

    public List<Map<String, Object>> executeWithStaging(QueryContext ctx, Function<String, String> sqlProvider) {
        String stagingTable = "staging_" + UUID.randomUUID().toString().replace("-", "");
        List<String> dims = ctx.getDimCodes();

        try (Connection conn = duckdbDs.getConnection()) {
            conn.setAutoCommit(false);
            Statement stmt = conn.createStatement();

            try {
                createStagingTable(stmt, stagingTable, dims);

                List<PhysicalTableReq> allTables = new ArrayList<>(ctx.getRequiredTables());
                for (int i = 0; i < allTables.size(); i += BATCH_SIZE) {
                    int end = Math.min(i + BATCH_SIZE, allTables.size());
                    List<PhysicalTableReq> batch = allTables.subList(i, end);
                    loadBatch(stmt, ctx, batch, stagingTable, dims);
                }

                conn.commit();

                try {
                    String sql = sqlProvider.apply(stagingTable);
                    return executeAndMap(stmt, sql);
                } finally {
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

    private void createStagingTable(Statement stmt, String tableName, List<String> dims) throws SQLException {
        StringBuilder ddl = new StringBuilder();
        ddl.append("CREATE TEMP TABLE ").append(tableName).append(" (");
        ddl.append("kpi_id VARCHAR, op_time VARCHAR, kpi_val DOUBLE");
        for (String dim : dims) {
            ddl.append(", ").append(dim).append(" VARCHAR");
        }
        ddl.append(")");
        stmt.execute(ddl.toString());
    }

    private void loadBatch(Statement stmt, QueryContext ctx, List<PhysicalTableReq> batch, String stagingTable,
            List<String> dims) throws Exception {
        // No need to attach

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

            String fromClause;
            if (alias.startsWith("read_parquet")) {
                fromClause = alias;
            } else {
                fromClause = alias + "." + sourceTable;
            }

            String insertSql = String.format(
                    "INSERT INTO %s (kpi_id, op_time, kpi_val%s) SELECT '%s', '%s', kpi_val%s FROM %s",
                    stagingTable, insertDims,
                    req.kpiId(), req.opTime(), smartSelect.toString(),
                    fromClause);
            stmt.execute(insertSql);
        }

        // Commit transaction
        stmt.getConnection().commit();
    }

    private List<Map<String, Object>> executeAndMap(Statement stmt, String sql) throws Exception {
        try (ResultSet rs = stmt.executeQuery(sql)) {
            ResultSetMetaData meta = rs.getMetaData();
            int colCount = meta.getColumnCount();
            List<Map<String, Object>> result = new ArrayList<>();
            while (rs.next()) {
                Map<String, Object> row = new HashMap<>(colCount);
                for (int i = 1; i <= colCount; i++) {
                    row.put(meta.getColumnLabel(i), rs.getObject(i));
                }
                result.add(row);
            }
            return result;
        }
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
