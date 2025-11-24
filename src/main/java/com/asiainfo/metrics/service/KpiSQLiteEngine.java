package com.asiainfo.metrics.service;

import com.asiainfo.metrics.model.db.DimDef;
import com.asiainfo.metrics.model.db.KpiDefinition;
import com.asiainfo.metrics.model.http.KpiQueryRequest;
import io.agroal.api.AgroalDataSource;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;
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
public class KpiSQLiteEngine extends AbstractKpiQueryEngineImpl {

    private static final Logger log = LoggerFactory.getLogger(KpiSQLiteEngine.class);
    private static final String NOT_EXISTS = "--";

    @Inject
    SQLiteFileManager sqliteFileManager;

    @Inject
    @io.quarkus.agroal.DataSource("sqlite")
    AgroalDataSource sqliteDataSource;

    @Override
    protected String getKpiDataTableName(String kpiId, String cycleType, String compDimCode, String opTime) {
        return sqliteFileManager.getSQLiteTableName(kpiId, opTime, compDimCode);
    }

    @Override
    protected String getDimDataTableName(String compDimCode) {
        // 维度表命名规则：dim_{compDimCode}
        return String.format("kpi_dim_%s", compDimCode);
    }

    @Override
    protected List<Map<String, Object>> doQuery(KpiQueryRequest request, Connection conn) throws Exception {
        // SQLite特殊处理：每个KPI都是独立的表，需要UNION ALL
        log.info("SQLite引擎执行查询：每个KPI单独表查询");

        List<String> kpiIds = request.kpiArray();
        boolean includeHistorical = request.includeHistoricalData() != null ? request.includeHistoricalData() : true;
        boolean includeTarget = request.includeTargetData() != null ? request.includeTargetData() : false;

        // 识别并处理虚拟指标表达式
        Map<String, String> virtualKpiMap = new HashMap<>(); // expression -> extracted kpi ids
        Map<String, String> regularKpiMap = new HashMap<>(); // kpiId -> kpiId

        for (String kpiId : kpiIds) {
            // 检查是否为虚拟指标表达式（包含${}语法）
            if (kpiId.contains("${")) {
                // 提取虚拟指标中的KPI依赖
                Set<String> dependentKpiIds = extractKpiIdsFromExpr(kpiId);
                String kpiIdsStr = String.join(",", dependentKpiIds);
                virtualKpiMap.put(kpiId, kpiIdsStr);
                log.debug("虚拟指标表达式: {}, 依赖KPI: {}", kpiId, dependentKpiIds);
            } else {
                regularKpiMap.put(kpiId, kpiId);
            }
        }

        // 收集所有需要的KPI定义（包括虚拟指标依赖的KPI）
        Set<String> allNeededKpiIds = new HashSet<>();
        allNeededKpiIds.addAll(regularKpiMap.keySet());
        for (String dependentKpiIds : virtualKpiMap.values()) {
            String[] kpiIdArray = dependentKpiIds.split(",");
            for (String kpiId : kpiIdArray) {
                allNeededKpiIds.add(kpiId.trim());
            }
        }

        // 获取KPI定义
        List<String> allNeededKpiIdList = new ArrayList<>(allNeededKpiIds);
        Map<String, KpiDefinition> allDefinitionsMap = metadataRepository.batchGetKpiDefinitions(allNeededKpiIdList);

        // 为每个KPI构建独立的查询，然后UNION ALL
        List<String> unionQueries = new ArrayList<>();

        // 处理普通KPI
        for (String kpiId : regularKpiMap.keySet()) {
            KpiDefinition kpiDef = allDefinitionsMap.get(kpiId);
            if (kpiDef == null) {
                log.warn("未找到KPI定义: {}", kpiId);
                continue;
            }

            // 为每个时间点构建查询
            List<String> timeQueries = new ArrayList<>();
            for (String opTime : request.opTimeArray()) {
                // SQLite中，每个KPI每个时间点都是独立的表
                String tableName = getKpiDataTableName(kpiId, kpiDef.cycleType(), kpiDef.compDimCode(), opTime);
                String sql = buildSqliteSingleKpiQuery(kpiDef, request, opTime, includeHistorical, includeTarget, "current", tableName, allDefinitionsMap);
                timeQueries.add(sql);

                if (includeHistorical) {
                    String lastCycleOpTime = calculateLastCycleTime(opTime);
                    String lastYearOpTime = calculateLastYearTime(opTime);
                    timeQueries.add(buildSqliteSingleKpiQuery(kpiDef, request, lastCycleOpTime, true, false, "lastCycle", tableName, allDefinitionsMap));
                    timeQueries.add(buildSqliteSingleKpiQuery(kpiDef, request, lastYearOpTime, true, false, "lastYear", tableName, allDefinitionsMap));
                }
            }

            // 合并该KPI所有时间点的查询
            String kpiQuery = String.join("\nUNION ALL\n", timeQueries);
            unionQueries.add(kpiQuery);
        }

        // 处理虚拟指标表达式
        for (Map.Entry<String, String> entry : virtualKpiMap.entrySet()) {
            String expression = entry.getKey();
            String dependentKpiIdsStr = entry.getValue();
            String[] dependentKpiIdArray = dependentKpiIdsStr.split(",");

            // 为每个时间点构建查询
            List<String> timeQueries = new ArrayList<>();
            for (String opTime : request.opTimeArray()) {
                List<String> kpiQueries = new ArrayList<>();

                // 为每个依赖KPI构建查询
                for (String kpiId : dependentKpiIdArray) {
                    kpiId = kpiId.trim();
                    KpiDefinition kpiDef = allDefinitionsMap.get(kpiId);
                    if (kpiDef == null) {
                        log.warn("虚拟指标依赖的KPI定义未找到: {}", kpiId);
                        continue;
                    }

                    String tableName = getKpiDataTableName(kpiId, kpiDef.cycleType(), kpiDef.compDimCode(), opTime);

                    // 为虚拟指标依赖的KPI生成简单的SELECT（不使用表达式转换）
                    List<String> dimCodeList = request.dimCodeArray() != null ? request.dimCodeArray() : new ArrayList<>();
                    String dimensionFields = buildDimensionFields(dimCodeList);

                    // 构建维度表JOIN语句（为每个维度都生成JOIN）
                    StringBuilder joinClauses = new StringBuilder();
                    for (String dimCode : dimCodeList) {
                        joinClauses.append(String.format("LEFT JOIN %s dim_%s on t.%s = dim_%s.dim_code\n",
                                getDimDataTableName(kpiDef.compDimCode()),
                                dimCode,
                                dimCode,
                                dimCode));
                    }

                    String selectFields;
                    if (dimensionFields.isEmpty()) {
                        selectFields = String.format("'%s' as kpi_id,\n                       '%s' as op_time,\n                       sum(t.kpi_val) as current,\n                       NULL as target_value,\nNULL as check_result,\nNULL as check_desc",
                                kpiId, opTime);
                    } else {
                        selectFields = String.format("%s,\n                       '%s' as kpi_id,\n                       '%s' as op_time,\n                       sum(t.kpi_val) as current,\n                       NULL as target_value,\nNULL as check_result,\nNULL as check_desc",
                                dimensionFields, kpiId, opTime);
                    }

                    String kpiQuery = String.format("""
                            SELECT
                                   %s
                            FROM %s t
                            %sWHERE 1=1
                              AND t.op_time = '%s'
                            %s
                            """,
                            selectFields,
                            tableName,
                            joinClauses.toString(),
                            opTime,
                            buildGroupByClauseForSingleQuery(dimCodeList).isEmpty() ? "" : "\n                GROUP BY " + buildGroupByClauseForSingleQuery(dimCodeList));

                    kpiQueries.add(kpiQuery);

                    if (includeHistorical) {
                        String lastCycleOpTime = calculateLastCycleTime(opTime);
                        String lastYearOpTime = calculateLastYearTime(opTime);

                        // 上期查询
                        String lastCycleSelectFields;
                        if (dimensionFields.isEmpty()) {
                            lastCycleSelectFields = String.format("'%s' as kpi_id,\n                       '%s' as op_time,\n                       sum(t.kpi_val) as current,\n                       NULL as target_value,\nNULL as check_result,\nNULL as check_desc",
                                    kpiId, lastCycleOpTime);
                        } else {
                            lastCycleSelectFields = String.format("%s,\n                       '%s' as kpi_id,\n                       '%s' as op_time,\n                       sum(t.kpi_val) as current,\n                       NULL as target_value,\nNULL as check_result,\nNULL as check_desc",
                                    dimensionFields, kpiId, lastCycleOpTime);
                        }

                        String lastCycleQuery = String.format("""
                                SELECT
                                       %s
                                FROM %s t
                                %sWHERE 1=1
                                  AND t.op_time = '%s'
                                %s
                                """,
                                lastCycleSelectFields,
                                tableName,
                                joinClauses.toString(),
                                lastCycleOpTime,
                                buildGroupByClauseForSingleQuery(dimCodeList).isEmpty() ? "" : "\n                GROUP BY " + buildGroupByClauseForSingleQuery(dimCodeList));

                        kpiQueries.add(lastCycleQuery);

                        // 去年同期查询
                        String lastYearSelectFields;
                        if (dimensionFields.isEmpty()) {
                            lastYearSelectFields = String.format("'%s' as kpi_id,\n                       '%s' as op_time,\n                       sum(t.kpi_val) as current,\n                       NULL as target_value,\nNULL as check_result,\nNULL as check_desc",
                                    kpiId, lastYearOpTime);
                        } else {
                            lastYearSelectFields = String.format("%s,\n                       '%s' as kpi_id,\n                       '%s' as op_time,\n                       sum(t.kpi_val) as current,\n                       NULL as target_value,\nNULL as check_result,\nNULL as check_desc",
                                    dimensionFields, kpiId, lastYearOpTime);
                        }

                        String lastYearQuery = String.format("""
                                SELECT
                                       %s
                                FROM %s t
                                %sWHERE 1=1
                                  AND t.op_time = '%s'
                                %s
                                """,
                                lastYearSelectFields,
                                tableName,
                                joinClauses.toString(),
                                lastYearOpTime,
                                buildGroupByClauseForSingleQuery(dimCodeList).isEmpty() ? "" : "\n                GROUP BY " + buildGroupByClauseForSingleQuery(dimCodeList));

                        kpiQueries.add(lastYearQuery);
                    }
                }

                // 合并所有依赖KPI的查询
                String combinedQuery = String.join("\nUNION ALL\n", kpiQueries);

                // 转换虚拟指标表达式为SQL（使用convertExpressionToSql支持时间修饰符）
                // 注意：外层查询引用内层聚合后的字段，所以useAggregatedField=true
                String convertedExpr = convertExpressionToSql(expression, "current", opTime, allDefinitionsMap, true);
                log.debug("虚拟指标表达式转换: {} -> {}", expression, convertedExpr);

                // 在外层计算表达式
                // 注意：外层查询不能直接引用JOIN的维度表，只能使用内层查询的别名
                String dimensionFields = buildDimensionFieldsForOuterQuery(request.dimCodeArray() != null ? request.dimCodeArray() : new ArrayList<>());
                String selectFields;
                if (dimensionFields.isEmpty()) {
                    selectFields = String.format("'%s' as kpi_id,\n                       '%s' as op_time,\n                       %s as current,\n                       NULL as target_value,\nNULL as check_result,\nNULL as check_desc",
                            expression, opTime, convertedExpr);
                } else {
                    selectFields = String.format("%s,\n                       '%s' as kpi_id,\n                       '%s' as op_time,\n                       %s as current,\n                       NULL as target_value,\nNULL as check_result,\nNULL as check_desc",
                            dimensionFields, expression, opTime, convertedExpr);
                }

                String virtualKpiSql = String.format("""
                        SELECT
                               %s
                        FROM (%s) t
                        """,
                        selectFields,
                        combinedQuery);

                timeQueries.add(virtualKpiSql);
            }

            // 合并该虚拟指标所有时间点的查询
            String kpiQuery = String.join("\nUNION ALL\n", timeQueries);
            unionQueries.add(kpiQuery);
        }

        // 合并所有KPI的查询
        String finalSql = String.join("\nUNION ALL\n", unionQueries);

        return executeQuery(finalSql, conn);
    }

    /**
     * 为SQLite构建单个KPI查询（不需要kpi_id过滤，因为每个KPI都是独立的表）
     */
    private String buildSqliteSingleKpiQuery(
            KpiDefinition kpiDef,
            KpiQueryRequest request,
            String opTime,
            boolean includeHistorical, boolean includeTarget, String targetTimePoint,
            String tableName,
            Map<String, KpiDefinition> allDefinitionsMap) {

        String cycleType = kpiDef.cycleType();
        String kpiId = kpiDef.kpiId();
        String compDimCode = kpiDef.compDimCode();

        String dimTable = getDimDataTableName(compDimCode);
        String targetTable = String.format("kpi_target_value_%s", compDimCode);

        // 获取查询维度
        List<String> groupByFields;
        if (request.dimCodeArray() != null && !request.dimCodeArray().isEmpty()) {
            groupByFields = new ArrayList<>(request.dimCodeArray());
        } else {
            groupByFields = new ArrayList<>();
        }

        // 构建维度描述字段
        String dimensionDescFields = buildDimensionFields(groupByFields);

        // 构建WHERE子句
        String whereClause = buildWhereClause(request);

        // 构建聚合表达式
        String currentExpr;
        String kpiType = kpiDef.kpiType();
        if ("extended".equalsIgnoreCase(kpiType)) {
            String aggFunc = kpiDef.aggFunc();
            if (aggFunc == null || aggFunc.isEmpty()) {
                aggFunc = "sum";
            }
            currentExpr = buildAggExpression(aggFunc, "t.kpi_val", opTime);
            log.debug("SQLite派生指标聚合函数: {} -> 表达式:{}", aggFunc, currentExpr);
        } else if ("computed".equalsIgnoreCase(kpiType) || "composite".equalsIgnoreCase(kpiType)) {
            String kpiExpr = kpiDef.kpiExpr();
            if (kpiExpr == null || kpiExpr.isEmpty()) {
                kpiExpr = NOT_EXISTS;
            }
            currentExpr = transformSql(kpiExpr, opTime, allDefinitionsMap);
            log.debug("SQLite计算指标表达式转换: {} -> opTime:{}", kpiExpr, currentExpr);
        } else {
            String kpiExpr = kpiDef.kpiExpr();
            if (kpiExpr == null || kpiExpr.isEmpty()) {
                kpiExpr = NOT_EXISTS;
            }
            currentExpr = convertExpressionToSql(kpiExpr, targetTimePoint, opTime, allDefinitionsMap);
        }

        // 构建目标值表JOIN条件和字段
        String targetFields;
        if (includeTarget) {
            targetFields = """
                NULL as target_value,
                NULL as check_result,
                NULL as check_desc""";
        } else {
            targetFields = """
                NULL as target_value,
                NULL as check_result,
                NULL as check_desc""";
        }

        // 时间点过滤条件（SQLite中不需要kpi_id过滤，因为每个表只有一个KPI）
        String timeFilter = String.format("AND t.op_time = '%s'", opTime);

        // 构建GROUP BY子句
        String groupByClauseForSql = buildGroupByClauseForSingleQuery(groupByFields);
        String groupBySql = groupByClauseForSql.isEmpty() ? "" : "\n                GROUP BY " + groupByClauseForSql;

        // 构建SELECT字段
        String selectFields;
        if (dimensionDescFields.isEmpty()) {
            selectFields = String.format("'%s' as kpi_id,\n                       '%s' as op_time,\n                       %s as current,\n                       %s",
                    kpiDef.kpiId(), opTime, currentExpr, targetFields);
        } else {
            selectFields = String.format("%s,\n                       '%s' as kpi_id,\n                       '%s' as op_time,\n                       %s as current,\n                       %s",
                    dimensionDescFields, kpiDef.kpiId(), opTime, currentExpr, targetFields);
        }

        return String.format("""
                SELECT
                       %s
                FROM %s t
                %s
                WHERE 1=1 %s
                  %s
                  %s
                """,
                selectFields,
                tableName,
                buildDimTableJoins(groupByFields, dimTable),
                whereClause,
                timeFilter,
                groupBySql);
    }

    @Override
    protected Connection getSQLiteConnection(KpiQueryRequest request) throws SQLException, IOException {
        String uniqueId = UUID.randomUUID().toString().replace("-", "");
        return DriverManager.getConnection("jdbc:sqlite:file:memdb_" + uniqueId + "?mode=memory");
    }

    @Override
    protected void preQuery(KpiQueryRequest request, Connection conn) throws Exception {
        // 收集所有需要的KPI定义（包括虚拟指标依赖的KPI）
        Set<String> allNeededKpiIds = new HashSet<>();
        Map<String, String> virtualKpiMap = new HashMap<>(); // expression -> dependent kpi ids

        // 识别虚拟指标表达式并提取依赖
        for (String kpiId : request.kpiArray()) {
            if (kpiId.contains("${")) {
                // 虚拟指标：提取依赖KPI
                Set<String> dependentKpiIds = extractKpiIdsFromExpr(kpiId);
                String kpiIdsStr = String.join(",", dependentKpiIds);
                virtualKpiMap.put(kpiId, kpiIdsStr);

                // 将依赖KPI加入待加载列表
                allNeededKpiIds.addAll(dependentKpiIds);
                log.debug("虚拟指标: {}, 依赖KPI: {}", kpiId, dependentKpiIds);
            } else {
                // 普通KPI：直接加入待加载列表
                allNeededKpiIds.add(kpiId);
            }
        }

        // 获取所有需要的KPI定义（包括虚拟指标依赖的KPI）
        List<String> allNeededKpiIdList = new ArrayList<>(allNeededKpiIds);
        Map<String, KpiDefinition> allKpiDefinitions = metadataRepository.batchGetKpiDefinitions(allNeededKpiIdList);

        Set<String> addedCompDims = new HashSet<>();
        Map<String, KpiDefinition> parsedKpiDefinitions = parseKpiDependencies(allKpiDefinitions);

        // 根据请求，装载所有需要的表

        // 每个指标（包括虚拟指标依赖的指标）
        for (Map.Entry<String, KpiDefinition> entry : parsedKpiDefinitions.entrySet()) {
            String kpiId = entry.getKey();
            KpiDefinition kpiDefinition = entry.getValue();
            String compDimCode = kpiDefinition.compDimCode();
            // 每个时间
            for (String opTime : request.opTimeArray()) {
                addDataTable(conn, opTime, kpiId, compDimCode);
            }

            // 如果要查询历史同比环比
            if(request.includeHistoricalData()) {
                for (String opTime : request.opTimeArray()) {
                    String lastCycle = calculateLastCycleTime(opTime);
                    String lastYear = calculateLastYearTime(opTime);
                    addDataTable(conn, lastCycle, kpiId, compDimCode);
                    addDataTable(conn, lastYear, kpiId, compDimCode);
                }
            }

            // 每个维度
            if(!addedCompDims.contains(compDimCode)) {
                addDimTable(conn, compDimCode);
                addedCompDims.add(compDimCode);
            }
        }
        // 如果要查询目标值
        if(request.includeTargetData()) {
            for (String compDimCode : addedCompDims) {
                addTargetTable(conn, compDimCode);
            }
        }
    }

    private void addTargetTable(Connection conn, String compDimCode) throws IOException, SQLException {
        try{
            String localPath = sqliteFileManager.downloadAndCacheTargetDB(compDimCode);
            try( Statement stmt = conn.createStatement();){
                stmt.execute("ATTACH '"+localPath+"' as temp_target_db");
                String tableName = sqliteFileManager.getSQLiteTargetTableName(compDimCode);
                stmt.execute("create table "+ tableName + " as select * from temp_target_db."+tableName);
                log.debug("添加目标值表至SQLite: {}", tableName);
            }
        }catch (RuntimeException e){
            if(e.getMessage()!=null && e.getMessage().contains(SQLiteFileManager.S3_FILE_NOT_EXISTS)){
                createEmptyTargetTable(conn, compDimCode);
            }else{
                throw e;
            }
        }
    }

    private void createEmptyTargetTable(Connection conn, String compDimCode) throws SQLException{
        try (Statement stmt = conn.createStatement();){
            String tableName = sqliteFileManager.getSQLiteTargetTableName(compDimCode);
            List<DimDef> dims = metadataRepository.getDimDefsByCompDim(compDimCode);
            String dimDef = dims.stream().map(dim -> dim.dbColName() + " varchar(32) ").collect(Collectors.joining(","));

            stmt.execute("""
                create table %s (
                op_time varchar(32),
                kpi_id varchar(32),
                %s,
                target_value varchar(32),
                check_result varchar(64),
                check_desc varchar(512),
                eff_start_date datetime,
                eff_end_date datetime
                )
            """.formatted(tableName, dimDef));
        }
    }

    private void addDimTable(Connection conn, String compDimCode) throws IOException, SQLException {
        try{
            String localPath = sqliteFileManager.downloadAndCacheDimDB(compDimCode);
            try (Statement stmt = conn.createStatement();){
                stmt.execute("ATTACH '" + localPath + "' as temp_dim_db");
                String tableName = sqliteFileManager.getSQLiteDimTableName(compDimCode);
                stmt.execute("create table "+ tableName + " as select * from temp_dim_db."+tableName);
                log.debug("添加维度表至SQLite: {}", tableName);
                stmt.execute("DETACH temp_dim_db");
            }
        }catch (RuntimeException e){
            if(e.getMessage()!=null && e.getMessage().contains(SQLiteFileManager.S3_FILE_NOT_EXISTS)){
                createEmptyDimTable(conn,  compDimCode);
            }else{
                throw e;
            }
        }
    }

    private void createEmptyDimTable(Connection conn, String compDimCode) throws SQLException{
        try (Statement stmt = conn.createStatement();) {
            String tableName = sqliteFileManager.getSQLiteDimTableName(compDimCode);
            stmt.execute("""
            create table %s (
                dim_code        varchar(32),
                dim_val         varchar(128),
                dim_id          varchar(32),
                parent_dim_code varchar(32)
            )
            """.formatted(tableName));
        }
    }

    private void createEmptyDataTable(Connection conn, String opTime, String kpiId, String compDimCode) throws SQLException {
        try (Statement stmt = conn.createStatement();){
            String tableName = sqliteFileManager.getSQLiteTableName(kpiId, opTime, compDimCode);
            List<DimDef> dims = metadataRepository.getDimDefsByCompDim(compDimCode);
            String dimDef = dims.stream().map(dim -> dim.dbColName() + " varchar(32) ").collect(Collectors.joining(","));

            stmt.execute("""
                create table %s (
                op_time varchar(32),
                kpi_id varchar(32),
                %s,
                kpi_val varchar(32)
                )
            """.formatted(tableName, dimDef));
        }
    }

    private void addDataTable(Connection conn, String opTime, String kpiId, String compDimCode) throws IOException, SQLException {
        try{
            String localPath = sqliteFileManager.downloadDataDB(kpiId, opTime, compDimCode);
            try (Statement stmt = conn.createStatement();){
                stmt.execute("ATTACH '" + localPath + "' as temp_data_db");
                String tableName = sqliteFileManager.getSQLiteTableName(kpiId, opTime, compDimCode);
                stmt.execute("create table "+ tableName + " as select * from temp_data_db."+tableName);
                log.debug("添加数据表至SQLite: {}", tableName);
                stmt.execute("DETACH temp_data_db");
            }
        }catch (RuntimeException e){ // 不存在时，创建一个空表保证SQL正常执行
            if(e.getMessage()!=null && e.getMessage().contains(SQLiteFileManager.S3_FILE_NOT_EXISTS)){
                createEmptyDataTable(conn, opTime, kpiId, compDimCode);
            }else{
                throw e;
            }

        }
    }
}
