package com.asiainfo.metrics.service;

import com.asiainfo.metrics.config.VirtualThreadConfig;
import com.asiainfo.metrics.model.KpiDefinition;
import com.asiainfo.metrics.model.KpiQueryRequest;
import com.asiainfo.metrics.model.KpiQueryResult;
import com.asiainfo.metrics.repository.KpiMetadataRepository;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.agroal.api.AgroalDataSource;
import io.quarkus.redis.datasource.RedisDataSource;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.eclipse.microprofile.config.ConfigProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * RDB引擎
 * 在数据向MinIO割接过程中，无法使用SQLite存算引擎时，使用元数据库承载存算需求
 * 所有计算在数据库层面完成，直接拼接kpiExpr到SQL中
 *
 * @author QvQ
 * @date 2025/11/11
 */
@ApplicationScoped
public class KpiRdbEngine implements KpiQueryEngine {

    private static final Logger log = LoggerFactory.getLogger(KpiRdbEngine.class);

    @Inject
    KpiMetadataRepository metadataRepository;

    @Inject
    VirtualThreadConfig virtualThreadConfig;

    @Inject
    @io.quarkus.agroal.DataSource("metadb")
    AgroalDataSource metadbDataSource;

    @Inject
    RedisDataSource redisDataSource;

    @Inject
    ObjectMapper objectMapper;

    @Override
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

    @Override
    public KpiQueryResult queryKpiData(KpiQueryRequest request) {
        // 3. 记录开始时间
        long startTime = System.currentTimeMillis();
        try {
            // 1. 验证请求参数
            if (request.kpiArray() == null || request.kpiArray().isEmpty()) {
                log.warn("KPI列表为空");
                return KpiQueryResult.error("KPI列表不能为空");
            }

            if (request.opTimeArray() == null || request.opTimeArray().isEmpty()) {
                log.warn("时间点列表为空");
                return KpiQueryResult.error("时间点列表不能为空");
            }

            log.info("开始查询KPI数据: {} 个KPI, {} 个时间点",
                    request.kpiArray().size(), request.opTimeArray().size());

            // 2. 尝试从缓存获取
            String cacheKey = generateCacheKey(request);
            KpiQueryResult cachedResult = getFromCache(cacheKey);
            if (cachedResult != null) {
                log.info("缓存命中: {}", cacheKey);
                // 缓存命中时，重新计算当前请求的耗时，但使用缓存的数据
                long cacheHitTime = System.currentTimeMillis() - startTime;
                String msg = String.format("查询成功！耗时 %d ms，共 %d 条记录",
                        cacheHitTime, cachedResult.dataArray().size());
                return KpiQueryResult.success(cachedResult.dataArray(), msg);
            }

            // 3. 缓存未命中，查询数据库
            log.info("缓存未命中，查询数据库: {}", cacheKey);

            // 4. 获取所有KPI的元数据定义
            List<String> kpiIds = request.kpiArray();
            Map<String, KpiDefinition> kpiMetadataMap = metadataRepository.batchGetKpiDefinitions(kpiIds);

            if (kpiMetadataMap.isEmpty()) {
                log.warn("未找到KPI定义: {}", kpiIds);
                return KpiQueryResult.error("未找到KPI定义");
            }

            // 4. 为每个KPI单独构建查询，然后UNION ALL
            List<String> unionQueries = new ArrayList<>();

            for (Map.Entry<String, KpiDefinition> entry : kpiMetadataMap.entrySet()) {
                String kpiId = entry.getKey();
                KpiDefinition kpiDef = entry.getValue();

                // 为每个时间点构建查询
                for (String opTime : request.opTimeArray()) {
                    // 计算三个时间点：当前、上期、去年
                    String currentOpTime = opTime;
                    String lastCycleOpTime = calculateLastCycleTime(opTime);
                    String lastYearOpTime = calculateLastYearTime(opTime);

                    log.debug("时间点: current={}, lastCycle={}, lastYear={}",
                            currentOpTime, lastCycleOpTime, lastYearOpTime);

                    // 4. 构建单个KPI单个时间点的SQL查询
                    String sql = buildSingleKpiSingleTimeQuery(kpiDef, request, currentOpTime, lastCycleOpTime, lastYearOpTime);
                    unionQueries.add(sql);
                }
            }

            // 5. 合并所有查询
            String finalSql = String.join("\nUNION ALL\n", unionQueries);

            // 输出SQL到控制台
            log.info("\n===== 最终SQL =====\n{}\n==================\n", finalSql);

            // 6. 执行SQL查询
            List<Map<String, Object>> allResults = executeQuery(finalSql);

            // 7. 构建结果并缓存
            long elapsedTime = System.currentTimeMillis() - startTime;
            String msg = String.format("查询成功！耗时 %d ms，共 %d 条记录",
                    elapsedTime, allResults.size());
            KpiQueryResult result = KpiQueryResult.success(allResults, msg);

            // 8. 将结果存入缓存
            putToCache(cacheKey, result);

            return result;

        } catch (Exception e) {
            log.error("查询KPI数据失败", e);
            return KpiQueryResult.error("查询失败: " + e.getMessage());
        }
    }

    /**
     * 计算上一周期时间
     */
    private String calculateLastCycleTime(String currentOpTime) {
        try {
            LocalDate current = LocalDate.parse(currentOpTime, DateTimeFormatter.ofPattern("yyyyMMdd"));
            LocalDate lastCycle = current.minusMonths(1);
            return lastCycle.format(DateTimeFormatter.ofPattern("yyyyMMdd"));
        } catch (Exception e) {
            log.warn("计算上一周期时间失败，使用当前时间: {}", currentOpTime, e);
            return currentOpTime;
        }
    }

    /**
     * 计算去年同期时间
     */
    private String calculateLastYearTime(String currentOpTime) {
        try {
            LocalDate current = LocalDate.parse(currentOpTime, DateTimeFormatter.ofPattern("yyyyMMdd"));
            LocalDate lastYear = current.minusYears(1);
            return lastYear.format(DateTimeFormatter.ofPattern("yyyyMMdd"));
        } catch (Exception e) {
            log.warn("计算去年同期时间失败，使用当前时间: {}", currentOpTime, e);
            return currentOpTime;
        }
    }

    /**
     * 构建单个KPI单个时间点的SQL查询
     * 参考SecureKpiSqlBuilder的思路
     */
    private String buildSingleKpiSingleTimeQuery(
            KpiDefinition kpiDef,
            KpiQueryRequest request,
            String currentOpTime, String lastCycleOpTime, String lastYearOpTime) {

        String cycleType = kpiDef.cycleType().toLowerCase();
        String compDimCode = kpiDef.compDimCode();
        String dataTable = String.format("kpi_%s_%s", cycleType, compDimCode);
        String dimTable = String.format("kpi_dim_%s", compDimCode);
        String targetTable = String.format("kpi_target_value_%s", compDimCode);

        // 获取查询维度（用于GROUP BY）
        List<String> groupByFields = request.dimCodeArray() != null ?
                new ArrayList<>(request.dimCodeArray()) : Arrays.asList("city_id", "county_id", "region_id");
        // 子查询中的GROUP BY（无表前缀）
        String groupByClause = groupByFields.stream()
                .collect(Collectors.joining(", "));
        // 外部查询中的GROUP BY（有表前缀）
        String groupByClauseWithAlias = groupByFields.stream()
                .map(field -> "t." + field)
                .collect(Collectors.joining(", "));

        // 根据KPI类型构建聚合表达式
        String currentExpr, lastYearExpr, lastCycleExpr;
        if ("extended".equalsIgnoreCase(kpiDef.kpiType())) {
            // 派生指标直接使用子查询中已经聚合的字段
            currentExpr = "t.current_val";
            lastYearExpr = "t.last_year_val";
            lastCycleExpr = "t.last_cycle_val";
        } else {
            // 其他类型使用kpiExpr
            String kpiExpr = kpiDef.kpiExpr();
            if (kpiExpr == null || kpiExpr.isEmpty()) {
                kpiExpr = "0";
            }
            currentExpr = kpiExpr;
            lastYearExpr = kpiExpr;
            lastCycleExpr = kpiExpr;
        }

        // 构建WHERE子句
        String whereClause = buildWhereClause(request);

        // 构建维度描述字段
        String dimensionDescFields = buildDimensionFields(groupByFields);

        return String.format("""
                SELECT
                       %s,
                       t.kpi_id,
                       t.op_time,
                       %s  as current,
                       %s  as last_year,
                       %s  as last_cycle,
                       target.target_value,
                       target.check_result,
                       target.check_desc
                FROM (SELECT %s,
                             kpi_id,
                             op_time,
                             sum(case when op_time = '%s' then kpi_val else 0 end) as current_val,
                             sum(case when op_time = '%s' then kpi_val else 0 end) as last_year_val,
                             sum(case when op_time = '%s' then kpi_val else 0 end) as last_cycle_val
                      FROM %s AS t
                      WHERE kpi_id = '%s'
                        and op_time IN ('%s', '%s', '%s')
                        %s
                      GROUP BY %s, kpi_id, op_time
                     ) t
                %s
                LEFT JOIN %s target
                  on t.kpi_id = target.kpi_id and t.op_time = target.op_time
                """,
                // 维度字段（包含ID和描述）
                dimensionDescFields,
                // 聚合表达式
                currentExpr, lastYearExpr, lastCycleExpr,
                // 子查询GROUP BY
                groupByClause,
                // 时间点
                currentOpTime, lastYearOpTime, lastCycleOpTime,
                // 数据表和KPI ID
                dataTable, kpiDef.kpiId(),
                // 时间点
                currentOpTime, lastYearOpTime, lastCycleOpTime,
                // 维度过滤
                whereClause,
                // 子查询GROUP BY
                groupByClause,
                // 维度表JOIN
                buildDimTableJoins(groupByFields, dimTable),
                // 目标值表
                targetTable);
    }

    /**
     * 构建单个KPI的查询
     * 核心：根据KPI类型构建不同的SQL结构
     * EXTENDED: 直接聚合查询原始数据
     * COMPUTED/EXPRESSION: 先查询依赖KPI，然后基于结果计算
     */
    private String buildSingleKpiQuery(
            KpiDefinition kpiDef,
            KpiQueryRequest request,
            String currentOpTime, String lastCycleOpTime, String lastYearOpTime) {

        // 根据KPI类型构建不同的SQL
        // 注意：这里所有类型都使用子查询结构，以确保字段别名正确
        // EXTENDED: 使用子查询结构
        // COMPUTED/EXPRESSION: 也使用子查询结构
        return buildSingleKpiSingleTimeQuery(kpiDef, request, currentOpTime, lastCycleOpTime, lastYearOpTime);
    }

    /**
     * 构建EXTENDED类型KPI的查询
     * 直接聚合查询，数据源就是当前KPI
     * 包含维度翻译：JOIN维度表获取描述字段
     */
    private String buildExtendedKpiQuery(
            KpiDefinition kpiDef,
            KpiQueryRequest request,
            String currentOpTime, String lastCycleOpTime, String lastYearOpTime) {

        String cycleType = kpiDef.cycleType().toLowerCase();
        String compDimCode = kpiDef.compDimCode();
        String dataTable = String.format("kpi_%s_%s", cycleType, compDimCode);
        String dimTable = String.format("kpi_dim_%s", compDimCode);
        String targetTable = String.format("kpi_target_value_%s", compDimCode);

        // 获取查询维度（用于GROUP BY）
        List<String> groupByFields = request.dimCodeArray() != null ?
                new ArrayList<>(request.dimCodeArray()) : Arrays.asList("city_id", "county_id", "region_id");
        // 给GROUP BY字段加表前缀
        String groupByClause = groupByFields.stream()
                .map(field -> "t." + field)
                .collect(Collectors.joining(", "));

        // 构建WHERE子句（维度过滤）
        String whereClause = buildWhereClause(request);

        // 构建维度描述字段
        String dimensionDescFields = buildDimensionFields(groupByFields);

        // 根据KPI类型构建不同的聚合表达式
        String currentExpr, lastYearExpr, lastCycleExpr;
        if ("extended".equalsIgnoreCase(kpiDef.kpiType())) {
            // 派生指标直接使用sum(kpi_val)获取指标值
            // 注意：这里在buildExtendedKpiQuery中，FROM是主表kpi_day_CD003 t，所以直接使用t.kpi_val
            currentExpr = String.format("sum(case when t.op_time = '%s' then t.kpi_val else 0 end)", currentOpTime);
            lastYearExpr = String.format("sum(case when t.op_time = '%s' then t.kpi_val else 0 end)", lastYearOpTime);
            lastCycleExpr = String.format("sum(case when t.op_time = '%s' then t.kpi_val else 0 end)", lastCycleOpTime);
        } else {
            // COMPUTED/EXPRESSION类型使用kpiExpr作为计算列
            String kpiExpr = kpiDef.kpiExpr();
            if (kpiExpr == null || kpiExpr.isEmpty()) {
                kpiExpr = "0";
            }
            // 转换kpiExpr中的KPI ID为聚合表达式（注意：这里不能使用时间点作为条件，因为是在同一张表中查询多个时间点）
            currentExpr = transformKpiExprToSql(kpiExpr, currentOpTime, lastYearOpTime, lastCycleOpTime)[0];
            lastYearExpr = transformKpiExprToSql(kpiExpr, currentOpTime, lastYearOpTime, lastCycleOpTime)[1];
            lastCycleExpr = transformKpiExprToSql(kpiExpr, currentOpTime, lastYearOpTime, lastCycleOpTime)[2];
        }

        return String.format("""
                SELECT
                       %s,
                       t.kpi_id,
                       '%s' as op_time,
                       %s  as current,
                       %s  as last_year,
                       %s  as last_cycle,
                       target.target_value,
                       target.check_result,
                       target.check_desc
                FROM %s t
                %s
                LEFT JOIN %s target
                  on %s
                WHERE t.kpi_id = '%s'
                  and t.op_time IN ('%s', '%s', '%s')
                  %s
                GROUP BY %s
                """,
                // 维度字段（包含ID和描述）
                dimensionDescFields,
                // op_time
                currentOpTime,
                // 直接使用sum(kpi_val)按时间点聚合
                currentExpr, lastYearExpr, lastCycleExpr,
                // 数据表（加别名t）
                dataTable,
                // 维度表JOIN
                buildDimTableJoins(groupByFields, dimTable),
                // 目标值表
                targetTable,
                // JOIN条件
                buildTargetJoinConditionForTarget(groupByFields, currentOpTime),
                // KPI ID
                kpiDef.kpiId(),
                // 时间点
                currentOpTime, lastYearOpTime, lastCycleOpTime,
                // 维度过滤条件
                whereClause,
                // GROUP BY（使用列别名，MySQL支持）
                buildGroupByClauseWithAliases(groupByFields) + ", t.kpi_id, t.op_time");
    }

    /**
     * 构建维度字段和描述字段（包含在GROUP BY中，不使用聚合函数）
     */
    private String buildDimensionFields(List<String> groupByFields) {
        StringBuilder fields = new StringBuilder();

        for (String field : groupByFields) {
            String descField = field + "_desc";
            String dimAlias = "dim_" + field;
            // 维度ID字段来自主表t，描述字段来自维度表JOIN
            fields.append(String.format("t.%s as %s, %s.dim_val as %s, ", field, field, dimAlias, descField));
        }

        // 移除最后的逗号和空格
        if (fields.length() > 0) {
            fields.setLength(fields.length() - 2);
        }

        return fields.toString();
    }

    /**
     * 构建包含维度描述字段的GROUP BY子句
     * 注意：GROUP BY中不能使用SELECT的别名，必须使用原始表达式
     */
    private String buildGroupByClauseWithAliases(List<String> groupByFields) {
        StringBuilder groupBy = new StringBuilder();
        for (String field : groupByFields) {
            // GROUP BY中必须使用原始表达式，不能用SELECT的别名
            groupBy.append("t.").append(field).append(", ");
            // 维度描述字段的原始表达式
            groupBy.append("dim_").append(field).append(".dim_val, ");
        }
        // 移除最后的逗号和空格
        if (groupBy.length() > 0) {
            groupBy.setLength(groupBy.length() - 2);
        }
        return groupBy.toString();
    }

    /**
     * 构建维度表JOIN
     */
    private String buildDimTableJoins(List<String> groupByFields, String dimTable) {
        StringBuilder joins = new StringBuilder();

        for (String field : groupByFields) {
            String alias = "dim_" + field;
            joins.append(String.format("LEFT JOIN %s %s on t.%s = %s.dim_code\n",
                    dimTable, alias, field, alias));
        }

        return joins.toString();
    }

    /**
     * 构建COMPUTED/EXPRESSION类型KPI的查询
     * 也使用基础查询，但kpiExpr会在更高层级作为计算列使用
     * 这里直接返回基础查询，让调用方在SQL中处理表达式
     */
    private String buildComputedKpiQuery(
            KpiDefinition kpiDef,
            KpiQueryRequest request,
            String currentOpTime, String lastCycleOpTime, String lastYearOpTime) {

        // COMPUTED/EXPRESSION类型也先查询基础数据
        // kpiExpr会在最终的SQL中作为计算列使用
        return buildExtendedKpiQuery(kpiDef, request, currentOpTime, lastCycleOpTime, lastYearOpTime);
    }

    /**
     * 为目标值表构建JOIN条件
     */
    private String buildTargetJoinConditionForTarget(List<String> groupByFields, String opTime) {
        StringBuilder joinCondition = new StringBuilder();
        joinCondition.append(String.format("t.kpi_id = target.kpi_id and t.op_time = '%s'", opTime));

        for (String field : groupByFields) {
            joinCondition.append(String.format(" and t.%s = target.%s", field, field));
        }

        return joinCondition.toString();
    }

    /**
     * 将kpiExpr转换为针对三个时间点的SQL表达式数组
     * 例如：kpiExpr = "KD1002 * 0.7 / (KD1002 + 100)"
     * 返回：[
     *   "sum(case when t.kpi_id = 'KD1002' and t.op_time = '20251106' then t.kpi_val else 0 end) * 0.7 / (sum(case when t.kpi_id = 'KD1002' and t.op_time = '20251106' then t.kpi_val else 0 end) + 100)",
     *   "sum(case when t.kpi_id = 'KD1002' and t.op_time = '20241006' then t.kpi_val else 0 end) * 0.7 / (sum(case when t.kpi_id = 'KD1002' and t.op_time = '20241006' then t.kpi_val else 0 end) + 100)",
     *   "sum(case when t.kpi_id = 'KD1002' and t.op_time = '20241006' then t.kpi_val else 0 end) * 0.7 / (sum(case when t.kpi_id = 'KD1002' and t.op_time = '20241006' then t.kpi_val else 0 end) + 100)"
     * ]
     */
    private String[] transformKpiExprToSql(String kpiExpr, String currentOpTime, String lastYearOpTime, String lastCycleOpTime) {
        String[] result = new String[3];

        // 转换当前时间点表达式
        result[0] = transformKpiExprForTimePoint(kpiExpr, currentOpTime);

        // 转换去年时间点表达式
        result[1] = transformKpiExprForTimePoint(kpiExpr, lastYearOpTime);

        // 转换上期时间点表达式
        result[2] = transformKpiExprForTimePoint(kpiExpr, lastCycleOpTime);

        return result;
    }

    /**
     * 将kpiExpr转换为针对特定时间点的SQL表达式
     */
    private String transformKpiExprForTimePoint(String kpiExpr, String opTime) {
        // 匹配KPI ID并替换为按时间点聚合的表达式
        Pattern pattern = Pattern.compile("\\b(K[DCYM]\\d{4})\\b");
        Matcher matcher = pattern.matcher(kpiExpr);

        StringBuffer sb = new StringBuffer();
        while (matcher.find()) {
            String kpiId = matcher.group(1);
            String replacement = String.format("sum(case when t.kpi_id = '%s' and t.op_time = '%s' then t.kpi_val else 0 end)", kpiId, opTime);
            matcher.appendReplacement(sb, replacement);
        }
        matcher.appendTail(sb);

        return sb.toString();
    }

    /**
     * 转换kpiExpr以引用正确的字段
     * 例如：sum(kpi_val) -> sum(current_val)
     * 或者：如果包含KPI ID引用，保留原样
     */
    private String convertKpiExprForField(String kpiExpr, String fieldName) {
        if (kpiExpr == null || kpiExpr.isEmpty()) {
            return "0";
        }

        // 如果kpiExpr包含KPI ID引用（如KD1002），说明是复合指标表达式，直接返回
        if (kpiExpr.matches(".*K[DCYM]\\d{4}.*")) {
            return kpiExpr;
        }

        // 否则，将kpi_val替换为对应的字段
        return kpiExpr.replace("kpi_val", fieldName);
    }

    /**
     * 构建WHERE子句（维度过滤）- 用于子查询，不需要表前缀
     */
    private String buildWhereClause(KpiQueryRequest request) {
        StringBuilder whereClause = new StringBuilder();

        // 添加维度条件（子查询中直接用字段名，不需要表前缀）
        if (request.dimConditionArray() != null && !request.dimConditionArray().isEmpty()) {
            for (KpiQueryRequest.DimCondition condition : request.dimConditionArray()) {
                String dimCode = condition.dimConditionCode();
                String dimVals = condition.dimConditionVal();
                whereClause.append(String.format(" AND %s IN (%s)", dimCode, dimVals));
            }
        }

        return whereClause.toString();
    }

    /**
     * 执行SQL查询并转换结果
     */
    private List<Map<String, Object>> executeQuery(String sql) throws SQLException {
        List<Map<String, Object>> resultList = new ArrayList<>();

        log.info("执行SQL: {}", sql);

        try (Connection conn = metadbDataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql);
             ResultSet rs = stmt.executeQuery()) {

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

        log.info("SQL查询完成，返回 {} 条记录", resultList.size());
        return resultList;
    }

    // ========== 缓存相关方法 ==========

    private static final String CACHE_PREFIX = "kpi:query:";
    private static final TypeReference<KpiQueryResult> RESULT_TYPE = new TypeReference<KpiQueryResult>() {};

    /**
     * 生成缓存Key
     */
    private String generateCacheKey(KpiQueryRequest request) {
        StringBuilder keyBuilder = new StringBuilder(CACHE_PREFIX);

        if (request.kpiArray() != null && !request.kpiArray().isEmpty()) {
            keyBuilder.append("kpi:").append(String.join(",", request.kpiArray()));
        }
        if (request.opTimeArray() != null && !request.opTimeArray().isEmpty()) {
            keyBuilder.append(":time:").append(String.join(",", request.opTimeArray()));
        }
        if (request.dimCodeArray() != null && !request.dimCodeArray().isEmpty()) {
            keyBuilder.append(":dim:").append(String.join(",", request.dimCodeArray()));
        }
        if (request.dimConditionArray() != null && !request.dimConditionArray().isEmpty()) {
            keyBuilder.append(":cond:");
            for (KpiQueryRequest.DimCondition cond : request.dimConditionArray()) {
                keyBuilder.append(cond.dimConditionCode())
                        .append("=")
                        .append(cond.dimConditionVal())
                        .append(",");
            }
        }
        return keyBuilder.toString();
    }

    /**
     * 从缓存获取数据
     */
    private KpiQueryResult getFromCache(String cacheKey) {
        try {
            String value = redisDataSource.value(String.class).get(cacheKey);
            if (value != null) {
                return objectMapper.readValue(value, RESULT_TYPE);
            }
        } catch (Exception e) {
            log.warn("从缓存获取数据失败: {}", cacheKey, e);
        }
        return null;
    }

    /**
     * 写入缓存
     */
    private void putToCache(String cacheKey, KpiQueryResult result) {
        try {
            String jsonResult = objectMapper.writeValueAsString(result);
            long ttlMinutes = ConfigProvider.getConfig()
                    .getValue("kpi.cache.ttl.minutes", Long.class);
            redisDataSource.value(String.class).setex(cacheKey, (int) (ttlMinutes * 60), jsonResult);
            log.info("查询结果已缓存: {}, TTL: {} 分钟", cacheKey, ttlMinutes);
        } catch (Exception e) {
            log.warn("写入缓存失败: {}", cacheKey, e);
        }
    }
}
