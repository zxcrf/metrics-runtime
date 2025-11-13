package com.asiainfo.metrics.service;

import com.asiainfo.metrics.config.VirtualThreadConfig;
import com.asiainfo.metrics.model.KpiDefinition;
import com.asiainfo.metrics.model.KpiQueryRequest;
import com.asiainfo.metrics.model.KpiQueryResult;
import com.asiainfo.metrics.repository.KpiMetadataRepository;
import io.agroal.api.AgroalDataSource;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.stream.Collectors;

/**
 * KPI计算引擎
 * 使用SQLite内存计算进行嵌套指标计算
 *
 * 核心特性:
 * - 分层计算: 通过临时表支持嵌套依赖
 * - 虚拟线程: 每个请求一个虚拟线程
 * - 内存优化: 使用内存SQLite数据库
 * - 水平扩展: 每个JVM实例独立处理
 */
@ApplicationScoped
public class KpiSQLiteEngine implements KpiQueryEngine {

    private static final Logger log = LoggerFactory.getLogger(KpiSQLiteEngine.class);
    private static final String NOT_EXISTS = "--";

    @Inject
    @io.quarkus.agroal.DataSource("sqlite")
    AgroalDataSource sqliteDataSource;

    @Inject
    @io.quarkus.agroal.DataSource("metadb")
    AgroalDataSource metadbDataSource;

    @Inject
    KpiMetadataRepository metadataRepository;

    @Inject
    VirtualThreadConfig virtualThreadConfig;

    @Inject
    SQLiteFileManager sqliteFileManager;

    /**
     * 异步查询KPI数据
     * 使用虚拟线程处理I/O密集型任务
     */
    public CompletableFuture<KpiQueryResult> queryKpiDataAsync(KpiQueryRequest request) {
        Executor executor = virtualThreadConfig.getComputeExecutor();

        return CompletableFuture.supplyAsync(() -> {
            try {
                log.info("开始查询KPI数据: {} 个KPI, {} 个时间点",
                        request.kpiArray().size(), request.opTimeArray().size());

                return queryKpiData(request);

            } catch (Exception e) {
                log.error("查询KPI数据失败", e);
                throw new RuntimeException("查询KPI数据失败", e);
            }
        }, executor);
    }

    /**
     * 同步查询KPI数据
     */
    public KpiQueryResult queryKpiData(KpiQueryRequest request) {
        try (Connection memConn = sqliteDataSource.getConnection()) {
            long startTime = System.currentTimeMillis();

            // 1. 验证请求参数
            if (request.kpiArray() == null || request.kpiArray().isEmpty()) {
                log.warn("KPI列表为空");
                return KpiQueryResult.error("KPI列表不能为空");
            }

            if (request.opTimeArray() == null || request.opTimeArray().isEmpty()) {
                log.warn("时间点列表为空");
                return KpiQueryResult.error("时间点列表不能为空");
            }

            // 2. 批量获取KPI定义
            Map<String, KpiDefinition> kpiDefinitions = metadataRepository.batchGetKpiDefinitions(request.kpiArray());

            // 3. 获取查询维度
            List<String> groupByFields = getGroupByFields(request);

            // 4. 为每个KPI构建查询并执行
            Map<String, Map<String, Object>> allKpiData = new HashMap<>();

            for (String kpiId : request.kpiArray()) {
                KpiDefinition kpiDef = kpiDefinitions.get(kpiId);
                if (kpiDef == null) {
                    log.warn("未找到KPI定义: {}", kpiId);
                    continue;
                }

                // 根据KPI类型选择处理方式
                Map<String, Object> kpiData;
                if ("extended".equalsIgnoreCase(kpiDef.kpiType())) {
                    // 派生指标：从SQLite文件查询
                    kpiData = queryExtendedKpi(memConn, kpiId, request, kpiDef);
                } else if ("computed".equalsIgnoreCase(kpiDef.kpiType())) {
                    // 计算指标：需要解析表达式并计算
                    kpiData = computeComputedKpi(memConn, kpiId, request, kpiDef, kpiDefinitions);
                } else {
                    // 简单指标：直接从SQLite文件查询
                    kpiData = querySimpleKpi(memConn, kpiId, request, kpiDef);
                }

                if (kpiData != null && !kpiData.isEmpty()) {
                    allKpiData.put(kpiId, kpiData);
                }
            }

            // 5. 转换为标准格式
            KpiQueryResult result = convertToStandardFormat(allKpiData, request);

            long elapsedTime = System.currentTimeMillis() - startTime;
            log.info("SQLite查询完成: 耗时 {} ms, 返回 {} 条记录", elapsedTime, result.dataArray().size());

            // 在msg中添加查询耗时
            String msgWithTime = String.format("查询成功！耗时 %d ms，共 %d 条记录", elapsedTime, result.dataArray().size());
            return KpiQueryResult.success(result.dataArray(), msgWithTime);

        } catch (Exception e) {
            log.error("SQLite计算KPI数据失败", e);
            return KpiQueryResult.error("计算失败: " + e.getMessage());
        }
    }


    /**
     * 获取查询维度
     */
    private List<String> getGroupByFields(KpiQueryRequest request) {
        if (request.dimCodeArray() != null && !request.dimCodeArray().isEmpty()) {
            return new ArrayList<>(request.dimCodeArray());
        } else {
            // 未指定维度时返回空列表，按时间汇总
            return new ArrayList<>();
        }
    }

    /**
     * 查询简单KPI指标
     */
    private Map<String, Object> querySimpleKpi(Connection memConn, String kpiId,
                                                KpiQueryRequest request, KpiDefinition def) throws SQLException {
        // 1. 下载SQLite文件
        String dbFile = downloadSqliteFile(kpiId, request, def);

        // 2. 附加数据库
        attachDatabase(memConn, dbFile);

        // 3. 构建查询SQL
        String querySql = buildSimpleKpiQuerySql(kpiId, request, def);

        // 4. 执行查询并处理结果
        Map<String, Object> result = executeQuery(memConn, querySql);

        // 5. 分离数据库
        detachDatabase(memConn);

        return result;
    }

    /**
     * 从SQLite文件查询派生指标
     */
    private Map<String, Object> queryExtendedKpi(Connection memConn, String kpiId,
                                                 KpiQueryRequest request, KpiDefinition def) throws SQLException {
        // 1. 下载SQLite文件
        String dbFile = downloadSqliteFile(kpiId, request, def);

        // 2. 附加数据库
        attachDatabase(memConn, dbFile);

        // 3. 构建查询SQL
        String querySql = buildExtendedKpiQuerySql(kpiId, request, def);

        // 4. 执行查询并处理结果
        Map<String, Object> result = executeQuery(memConn, querySql);

        // 5. 分离数据库
        detachDatabase(memConn);

        return result;
    }

    /**
     * 计算复合指标
     */
    private Map<String, Object> computeComputedKpi(Connection memConn, String kpiId,
                                                     KpiQueryRequest request, KpiDefinition def,
                                                     Map<String, KpiDefinition> kpiDefinitions) throws SQLException {
        // 1. 下载SQLite文件
        String dbFile = downloadSqliteFile(kpiId, request, def);

        // 2. 附加数据库
        attachDatabase(memConn, dbFile);

        // 3. 解析表达式并计算
        String querySql = buildComputedKpiQuerySql(kpiId, request, def, kpiDefinitions);

        // 4. 执行查询并处理结果
        Map<String, Object> result = executeQuery(memConn, querySql);

        // 5. 分离数据库
        detachDatabase(memConn);

        return result;
    }

    /**
     * 附加数据库
     */
    private void attachDatabase(Connection memConn, String dbFile) throws SQLException {
        String attachSql = "ATTACH DATABASE '" + dbFile + "' AS data_db";
        try (Statement stmt = memConn.createStatement()) {
            stmt.execute(attachSql);
        }
    }

    /**
     * 分离数据库
     */
    private void detachDatabase(Connection memConn) throws SQLException {
        memConn.createStatement().execute("DETACH DATABASE data_db");
    }

    /**
     * 下载SQLite文件
     */
    private String downloadSqliteFile(String kpiId, KpiQueryRequest request, KpiDefinition def) {
        String timeRange = request.opTimeArray().getFirst();
        String compDimCode = def.compDimCode();
        try {
            return sqliteFileManager.downloadAndCacheDB(kpiId, timeRange, compDimCode);
        } catch (Exception e) {
            log.error("下载SQLite文件失败: {}", kpiId, e);
            throw new RuntimeException("下载SQLite文件失败", e);
        }
    }

    /**
     * 构建简单KPI查询SQL
     */
    private String buildSimpleKpiQuerySql(String kpiId, KpiQueryRequest request, KpiDefinition def) {
        // 构建正确的表名：kpi_{kpiId}_{opTime}_{compDimCode}
        String opTime = request.opTimeArray().getFirst();
        String tableName = String.format("kpi_%s_%s_%s", kpiId, opTime, def.compDimCode());

        StringBuilder sql = new StringBuilder();
        sql.append("SELECT * FROM data_db.").append(tableName).append(" WHERE kpi_id = '").append(kpiId).append("'");

        if (request.opTimeArray() != null && !request.opTimeArray().isEmpty()) {
            String times = request.opTimeArray().stream()
                    .map(t -> "'" + t + "'")
                    .collect(Collectors.joining(","));
            sql.append(" AND op_time IN (").append(times).append(")");
        }

        // 添加维度过滤
        if (request.dimConditionArray() != null && !request.dimConditionArray().isEmpty()) {
            for (KpiQueryRequest.DimCondition condition : request.dimConditionArray()) {
                sql.append(" AND ").append(condition.dimConditionCode())
                   .append(" IN (").append(condition.dimConditionVal()).append(")");
            }
        }

        return sql.toString();
    }

    /**
     * 构建派生KPI查询SQL
     */
    private String buildExtendedKpiQuerySql(String kpiId, KpiQueryRequest request, KpiDefinition def) {
        return buildSimpleKpiQuerySql(kpiId, request, def);
    }

    /**
     * 构建复合KPI查询SQL
     */
    private String buildComputedKpiQuerySql(String kpiId, KpiQueryRequest request, KpiDefinition def,
                                            Map<String, KpiDefinition> kpiDefinitions) {
        // TODO: 实现复合指标SQL构建逻辑
        // 这里需要解析表达式，替换KPI引用为子查询
        return buildSimpleKpiQuerySql(kpiId, request, def);
    }

    /**
     * 执行SQL查询
     */
    private Map<String, Object> executeQuery(Connection memConn, String querySql) throws SQLException {
        Map<String, Object> result = new HashMap<>();
        List<Map<String, Object>> data = new ArrayList<>();

        log.debug("执行SQL: {}", querySql);

        try (Statement stmt = memConn.createStatement();
             ResultSet rs = stmt.executeQuery(querySql)) {

            ResultSetMetaData md = rs.getMetaData();
            int columnCount = md.getColumnCount();

            while (rs.next()) {
                Map<String, Object> row = new HashMap<>();
                for (int i = 1; i <= columnCount; i++) {
                    String colName = md.getColumnName(i);
                    Object value = rs.getObject(i);
                    row.put(colName, value);
                }
                data.add(row);
            }
        }

        result.put("data", data);
        log.debug("查询完成，返回 {} 条记录", data.size());
        return result;
    }

    /**
     * 转换为标准格式
     */
    private KpiQueryResult convertToStandardFormat(Map<String, Map<String, Object>> allKpiData, KpiQueryRequest request) {
        List<Map<String, Object>> resultData = new ArrayList<>();

        // 获取维度字段
        List<String> dimFields = getGroupByFields(request);

        // 按维度分组聚合数据
        Map<String, Map<String, Object>> aggregatedMap = new LinkedHashMap<>();

        for (Map.Entry<String, Map<String, Object>> entry : allKpiData.entrySet()) {
            String kpiId = entry.getKey();
            @SuppressWarnings("unchecked")
            List<Map<String, Object>> kpiData = (List<Map<String, Object>>) entry.getValue().get("data");

            for (Map<String, Object> row : kpiData) {
                // 构建维度组合的Key
                StringBuilder dimKeyBuilder = new StringBuilder();
                for (String dimField : dimFields) {
                    Object dimValue = row.get(dimField);
                    dimKeyBuilder.append(dimField).append("=").append(dimValue).append("|");
                }
                String dimKey = dimKeyBuilder.toString();

                // 获取或创建聚合记录
                Map<String, Object> aggregatedRow = aggregatedMap.computeIfAbsent(dimKey, k -> {
                    Map<String, Object> newRow = new LinkedHashMap<>();
                    // 复制维度字段
                    for (String dimField : dimFields) {
                        newRow.put(dimField, row.get(dimField));
                        // 添加维度描述字段
                        String descField = dimField + "_desc";
                        if (row.containsKey(descField)) {
                            newRow.put(descField, row.get(descField));
                        }
                    }
                    newRow.put("opTime", request.opTimeArray().get(0));
                    newRow.put("kpiValues", new LinkedHashMap<String, Map<String, Object>>());
                    return newRow;
                });

                // 构建KPI值对象
                Map<String, Object> kpiValueMap = new LinkedHashMap<>();
                // 从kpi_val列获取当前值
                Object currentValue = row.get("kpi_val");
                kpiValueMap.put("current", currentValue != null ? currentValue : NOT_EXISTS);
                kpiValueMap.put("lastYear", NOT_EXISTS);  // SQLite文件只存储当前周期数据
                kpiValueMap.put("lastCycle", NOT_EXISTS);  // 历史数据需要在查询时动态计算

                // 添加到kpiValues
                @SuppressWarnings("unchecked")
                Map<String, Map<String, Object>> kpiValues = (Map<String, Map<String, Object>>) aggregatedRow.get("kpiValues");
                kpiValues.put(kpiId, kpiValueMap);
            }
        }

        resultData.addAll(aggregatedMap.values());

        String msg = String.format("查询成功！共 %d 条记录", resultData.size());
        return KpiQueryResult.success(resultData, msg);
    }
}
