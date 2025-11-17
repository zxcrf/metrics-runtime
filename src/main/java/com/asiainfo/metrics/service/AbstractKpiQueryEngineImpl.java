package com.asiainfo.metrics.service;

import com.asiainfo.metrics.model.KpiRowMaper;
import com.asiainfo.metrics.model.db.KpiDefinition;
import com.asiainfo.metrics.model.http.KpiQueryRequest;
import com.asiainfo.metrics.model.http.KpiQueryResult;
import com.asiainfo.metrics.repository.KpiMetadataRepository;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.agroal.api.AgroalDataSource;
import io.quarkus.redis.datasource.RedisDataSource;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.eclipse.microprofile.config.ConfigProvider;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 *
 *
 * @author QvQ
 * @date 2025/11/17
 */
@ApplicationScoped
public class AbstractKpiQueryEngineImpl extends AbstractKpiQueryEngine {

    private static final Logger log = LoggerFactory.getLogger(AbstractKpiQueryEngineImpl.class);

    @Inject
    KpiMetadataRepository metadataRepository;

    @Inject
    @io.quarkus.agroal.DataSource("metadb")
    AgroalDataSource metadbDataSource;

    @Inject
    RedisDataSource redisDataSource;

    @Inject
    ObjectMapper objectMapper;

    @Override
    protected List<Map<String, Object>> doQuery(KpiQueryRequest request) throws Exception {
        // 正常KPI查询流程
        List<String> kpiIds = request.kpiArray();

        // 是否包含历史数据（lastCycle和lastYear）
        // 默认true，但可以通过request.includeHistoricalData()覆盖
        boolean includeHistorical = request.includeHistoricalData() != null ? request.includeHistoricalData() : true;
        // 是否包含目标值相关数据（target_value、check_result、check_desc）
        // 默认false，但可以通过request.includeTargetData()覆盖
        boolean includeTarget = request.includeTargetData() != null ? request.includeTargetData() : false;

        // 批量获取所有非表达式KPI定义（避免循环中查询数据库）
        List<String> regularKpiIds = kpiIds.stream()
                .filter(kpiId -> !isComplexExpression(kpiId))
                .collect(Collectors.toList());

        Map<String, KpiDefinition> allKpiDefinitions = metadataRepository.batchGetKpiDefinitions(regularKpiIds);

        // 为每个KPI构建一个查询（包含所有时间点）
        List<String> unionQueries = new ArrayList<>();

        // 处理每个KPI ID（包括复杂表达式和普通KPI）
        for (String kpiId : kpiIds) {
            // 检查是否为复杂表达式
            if (isComplexExpression(kpiId)) { //TODO 先写完派生指标
                log.info("检测到复杂表达式: {}", kpiId);
                // 复杂表达式当作特殊的计算指标处理
                // 提取实际的KPI ID（去掉${}包装）用于标识，实际表达式保存在kpiExpr中
                String actualKpiId = extractActualKpiId(kpiId);
                KpiDefinition pseudoKpiDef = new KpiDefinition(
                        actualKpiId, kpiId, "EXPRESSION", "CD003", "DAY", null, null, kpiId, null, null
                );

                // 为每个时间点构建查询
                List<String> timeQueries = new ArrayList<>();
                for (String opTime : request.opTimeArray()) {
                    String currentOpTime = opTime;
                    String lastCycleOpTime = includeHistorical ? calculateLastCycleTime(opTime) : null;
                    String lastYearOpTime = includeHistorical ? calculateLastYearTime(opTime) : null;

                    log.debug("复杂表达式时间点: current={}, lastCycle={}, lastYear={}",
                            currentOpTime, lastCycleOpTime, lastYearOpTime);

                    // 构建复杂表达式的SQL查询
                    String sql = buildExpressionQuery(pseudoKpiDef, request, currentOpTime, lastCycleOpTime, lastYearOpTime, includeHistorical, includeTarget);
                    timeQueries.add(sql);
                }

                // 合并该表达式所有时间点的查询
                String exprQuery = String.join("\nUNION ALL\n", timeQueries);
                unionQueries.add(exprQuery);

            } else {
                // 普通KPI：直接从批量查询结果中获取（无数据库IO）
                KpiDefinition kpiDef = allKpiDefinitions.get(kpiId);

                if (kpiDef == null) {
                    log.warn("未找到KPI定义: {}", kpiId);
                    continue; // 跳过未定义的KPI
                }

                // 为每个时间点构建查询
                List<String> timeQueries = new ArrayList<>();
                for (String opTime : request.opTimeArray()) {
                    String currentOpTime = opTime;
                    String lastCycleOpTime = includeHistorical ? calculateLastCycleTime(opTime) : null;
                    String lastYearOpTime = includeHistorical ? calculateLastYearTime(opTime) : null;

                    log.debug("KPI {} 时间点: current={}, lastCycle={}, lastYear={}",
                            kpiId, currentOpTime, lastCycleOpTime, lastYearOpTime);

                    // 构建单个KPI单个时间点的SQL查询
//                    String sql = buildSingleKpiSingleTimeQuery(kpiDef, request, currentOpTime, lastCycleOpTime, lastYearOpTime, includeHistorical, includeTarget);
                    String sql = buildSingleKpiSingleTimeQuery(kpiDef, request, currentOpTime, includeHistorical, includeTarget);
                    timeQueries.add(sql);
                    if(includeHistorical) {
                        timeQueries.add(buildSingleKpiSingleTimeQuery(kpiDef, request, lastCycleOpTime, includeHistorical, false));
                        timeQueries.add(buildSingleKpiSingleTimeQuery(kpiDef, request, lastYearOpTime, includeHistorical, false));
                    }
                }

                // 合并该KPI所有时间点的查询
                String kpiQuery = String.join("\nUNION ALL\n", timeQueries);
                unionQueries.add(kpiQuery);
            }
        }

        // 合并所有KPI的查询
        String finalSql = String.join("\nUNION ALL\n", unionQueries);

        // 执行SQL查询
        return executeQuery(finalSql);
    }

    /**
     * 构建复杂表达式的SQL查询
     * 将表达式转换为SQL子查询，直接拼接到主SQL中
     */
    protected String buildExpressionQuery(
            KpiDefinition kpiDef,
            KpiQueryRequest request,
            String currentOpTime, String lastCycleOpTime, String lastYearOpTime,
            boolean includeHistorical, boolean includeTarget) {

        String expression = kpiDef.kpiExpr(); // 表达式本身就是kpiExpr

        // 获取查询维度（用于GROUP BY）
        // 注意：需要同时检查null和空数组的情况
        List<String> groupByFields;
        if (request.dimCodeArray() != null && !request.dimCodeArray().isEmpty()) {
            groupByFields = new ArrayList<>(request.dimCodeArray());
            log.info("使用指定的维度字段: {}", groupByFields);
        } else {
            // 用户未指定维度时不使用默认维度，而是按时间汇总
            groupByFields = new ArrayList<>();
            log.info("未指定维度字段，按时间汇总（groupByFields为空）");
        }

        // 构建维度描述字段
        String dimensionFields = buildDimensionFields(groupByFields);

        // 构建WHERE子句
        String whereClause = buildWhereClause(request);

        // 将复杂表达式转换为SQL
        // 表达式格式：${KD2001.lastCycle} / (${KD1002}+${KD1005})
        // 需要为 current、last_year、last_cycle 三列分别转换表达式
        String currentExpr = convertExpressionToSql(expression, "current", currentOpTime, lastCycleOpTime, lastYearOpTime);
        String lastYearExpr = includeHistorical ? convertExpressionToSql(expression, "lastYear", currentOpTime, lastCycleOpTime, lastYearOpTime) : "'--'";
        String lastCycleExpr = includeHistorical ? convertExpressionToSql(expression, "lastCycle", currentOpTime, lastCycleOpTime, lastYearOpTime) : "'--'";

        log.debug("表达式转换结果: expression={}, currentExpr={}, lastYearExpr={}, lastCycleExpr={}",
                expression, currentExpr, lastYearExpr, lastCycleExpr);

        // 构建分组字段
        String groupByClause = groupByFields.stream()
                .collect(Collectors.joining(", "));

        // 构建目标值相关字段
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

        // 构建时间点过滤条件
        String timeFilter;
        if (includeHistorical) {
            // 复杂表达式需要查询依赖的KPI数据，所以需要包含历史时间点
            timeFilter = String.format("AND t.op_time IN ('%s', '%s', '%s')",
                    currentOpTime, lastYearOpTime, lastCycleOpTime);
        } else {
            timeFilter = String.format("AND t.op_time = '%s'", currentOpTime);
        }

        // 构建GROUP BY子句（当没有维度时不生成GROUP BY）
        String groupByClauseSql = groupByClause.isEmpty() ? "" : "\nGROUP BY " + buildGroupByClauseForSingleQuery(groupByFields);

        // 构建SELECT字段（处理空维度字段的情况）
        String selectFields;
        if (dimensionFields.isEmpty()) {
            selectFields = String.format("'%s' as kpi_id,\n                       '%s' as op_time,\n                       %s as current,\n                       %s as last_year,\n                       %s as last_cycle,\n                       %s",
                    kpiDef.kpiId(), currentOpTime, currentExpr, lastYearExpr, lastCycleExpr, targetFields);
        } else {
            selectFields = String.format("%s,\n                       '%s' as kpi_id,\n                       '%s' as op_time,\n                       %s as current,\n                       %s as last_year,\n                       %s as last_cycle,\n                       %s",
                    dimensionFields, kpiDef.kpiId(), currentOpTime, currentExpr, lastYearExpr, lastCycleExpr, targetFields);
        }

        // 生成带注释的SQL，便于调试
        String sql = String.format("""
                /* currentExpr: %s */
                /* lastYearExpr: %s */
                /* lastCycleExpr: %s */
                SELECT
                       %s
                FROM %s t
                %s
                WHERE 1=1 %s
                  %s%s
                """,
                currentExpr, lastYearExpr, lastCycleExpr,
                selectFields,
                // 使用动态获取的表名，不再硬编码
                "kpi_" + kpiDef.cycleType().toLowerCase() + "_" + kpiDef.compDimCode(),
                buildDimTableJoins(groupByFields, "kpi_dim_" + kpiDef.compDimCode()),
                whereClause,
                timeFilter,
                groupByClauseSql);

        log.info("复杂表达式SQL:\n{}", sql);
        return sql;
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
     * 构建单个KPI单个时间点的SQL查询
     * 参考SecureKpiSqlBuilder的分表逻辑，但返回单行数据（current/last_year/last_cycle三列）
     */
    private String buildSingleKpiSingleTimeQuery(
            KpiDefinition kpiDef,
            KpiQueryRequest request,
            String opTime, /*String lastCycleOpTime, String lastYearOpTime,*/
            boolean includeHistorical, boolean includeTarget) {

        String cycleType = kpiDef.cycleType();
        String kpiId = kpiDef.kpiId();
        String compDimCode = kpiDef.compDimCode();

        String dimTable = getDimDataTableName(compDimCode);
        String kpiDataTable = getKpiDataTableName(kpiId, cycleType, compDimCode, opTime);
        String targetTable = String.format("kpi_target_value_%s", compDimCode);

        // 获取查询维度（用于GROUP BY）
        // 注意：需要同时检查null和空数组的情况
        List<String> groupByFields;
        if (request.dimCodeArray() != null && !request.dimCodeArray().isEmpty()) {
            groupByFields = new ArrayList<>(request.dimCodeArray());
            log.info("使用指定的维度字段: {}", groupByFields);
        } else {
            // 用户未指定维度时不使用默认维度，而是按时间汇总
            groupByFields = new ArrayList<>();
            log.info("未指定维度字段，按时间汇总（groupByFields为空）");
        }

        // 构建维度描述字段
        String dimensionDescFields = buildDimensionFields(groupByFields);

        // 构建WHERE子句
        String whereClause = buildWhereClause(request);

        // 根据KPI类型构建聚合表达式
        String currentExpr/*, lastYearExpr, lastCycleExpr*/;
        if ("extended".equalsIgnoreCase(kpiDef.kpiType())) {
            // 派生指标：使用原始kpi_val进行聚合
            currentExpr = "sum(case when t.op_time = '" + opTime + "' then t.kpi_val else null end)";
//            if (includeHistorical) {
//                lastYearExpr = "sum(case when t.op_time = '" + lastYearOpTime + "' then t.kpi_val else null end)";
//                lastCycleExpr = "sum(case when t.op_time = '" + lastCycleOpTime + "' then t.kpi_val else null end)";
//            } else {
//                lastYearExpr = "'"+ NOT_EXISTS +"'";
//                lastCycleExpr = "'"+ NOT_EXISTS +"'";
//            }
        } else if ("computed".equalsIgnoreCase(kpiDef.kpiType())) { // TODO 计算指标要额外获取依赖
            // 计算指标：需要解析表达式
            String kpiExpr = kpiDef.kpiExpr();
            if (kpiExpr == null || kpiExpr.isEmpty()) {
                kpiExpr = NOT_EXISTS;
            }

            // 转换表达式为SQL
            currentExpr = transformSql(kpiExpr, opTime);
//            if (includeHistorical) {
//                lastYearExpr = transformSql(kpiExpr, lastYearOpTime);
//                lastCycleExpr = transformSql(kpiExpr, lastCycleOpTime);
//            } else {
//                lastYearExpr = "'"+ NOT_EXISTS +"'";
//                lastCycleExpr = "'"+ NOT_EXISTS +"'";
//            }

            log.debug("计算指标表达式转换: {} -> opTime:{}", kpiExpr, currentExpr);
        } else {
            // 其他类型使用kpiExpr
            String kpiExpr = kpiDef.kpiExpr();
            if (kpiExpr == null || kpiExpr.isEmpty()) {
                kpiExpr = NOT_EXISTS;
            }
            currentExpr = kpiExpr;
//            if (includeHistorical) {
//                lastYearExpr = kpiExpr;
//                lastCycleExpr = kpiExpr;
//            } else {
//                lastYearExpr = "'"+ NOT_EXISTS +"'";
//                lastCycleExpr = "'"+ NOT_EXISTS +"'";
//            }
        }
        // 构建目标值表JOIN条件和字段
        String targetJoinCondition = null;
        String targetFields = null;
        String targetJoinClause = null;

        if (includeTarget) {
            targetJoinCondition = buildTargetJoinConditionForTarget(groupByFields, opTime);
            targetFields = """
                MAX(target.target_value) as target_value,
                MAX(target.check_result) as check_result,
                MAX(target.check_desc) as check_desc""";
            targetJoinClause = String.format("""
                LEFT JOIN %s target
                  on %s""", targetTable, targetJoinCondition);
        } else {
            targetFields = """
                NULL as target_value,
                NULL as check_result,
                NULL as check_desc""";
            targetJoinClause = "";
        }

        // 构建时间点过滤条件
        String timeFilter;
//        if (includeHistorical) {
//            timeFilter = String.format("AND t.op_time IN ('%s', '%s', '%s')",
//                    opTime, lastYearOpTime, lastCycleOpTime);
//        } else {
//            timeFilter = String.format("AND t.op_time = '%s'", opTime);
//        }
        timeFilter = String.format("AND t.op_time = '%s'", opTime);

        // 构建GROUP BY子句（当没有维度时不生成GROUP BY）
        String groupByClauseForSql = buildGroupByClauseForSingleQuery(groupByFields);
        String groupBySql = groupByClauseForSql.isEmpty() ? "" : "\n                GROUP BY " + groupByClauseForSql;

        // 构建SELECT字段（处理空维度字段的情况）
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
                %s
                WHERE t.kpi_id %s
                  %s
                  %s%s
                """,
                selectFields,
                kpiDataTable,
                buildDimTableJoins(groupByFields, dimTable),
                targetJoinClause,
                // 对于计算指标，使用依赖的KPI过滤；对于扩展指标，使用自身ID
                ("computed".equalsIgnoreCase(kpiDef.kpiType()) ?
                        "IN ('" + String.join("','", extractKpiIdsFromExpr(kpiDef.kpiExpr())) + "')" :
                        "= '" + kpiDef.kpiId() + "'"),
                timeFilter,
                whereClause,
                groupBySql);
    }


    /**
     * 构建WHERE子句（维度过滤）- 用于主查询，需要表前缀
     */
    private String buildWhereClause(KpiQueryRequest request) {
        StringBuilder whereClause = new StringBuilder();

        // 添加维度条件（主查询中需要表前缀t.）
        if (request.dimConditionArray() != null && !request.dimConditionArray().isEmpty()) {
            for (KpiQueryRequest.DimCondition condition : request.dimConditionArray()) {
                String dimCode = condition.dimConditionCode();
                String dimVals = condition.dimConditionVal();
                whereClause.append(String.format(" AND t.%s IN (%s)", dimCode, dimVals));
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

            KpiRowMaper.sqlRowMapping(resultList, rs);
        }

        log.info("SQL查询完成，返回 {} 条记录", resultList.size());
        return resultList;
    }



    /**
     * 将复杂表达式转换为SQL
     * 例如: ${KD2001.lastCycle} / (${KD1002}+${KD1005})
     * 转换为子查询
     * @param targetTimePoint 指定要查询的时间点：current、last_cycle、last_year
     */
    private String convertExpressionToSql(String expression, String targetTimePoint, String currentOpTime, String lastCycleOpTime, String lastYearOpTime) {
        // 提取表达式中的KPI引用
        List<KpiMetadataRepository.KpiReference> kpiRefs = metadataRepository.extractKpiReferences(expression);

        String sqlExpression = expression;

        // 将每个KPI引用替换为子查询
        for (KpiMetadataRepository.KpiReference ref : kpiRefs) {
            // 注意：extractKpiReferences会为没有时间修饰符的表达式默认添加"current"修饰符
            // 但我们这里需要根据调用时传入的targetTimePoint决定查询哪个时间点
            // 只有当用户明确指定时间修饰符时（例如${KD1002.lastYear}），才使用修饰符

            // 检查原始表达式中是否包含时间修饰符
            String fullRef = ref.fullReference();
            boolean hasExplicitTimeModifier = fullRef.contains(".");

            // 确定要查询的时间点
            String queryTimePoint;
            if (hasExplicitTimeModifier) {
                // 用户明确指定了时间修饰符，使用它
                queryTimePoint = ref.timeModifier();
            } else {
                // 用户没有指定时间修饰符，根据传入的targetTimePoint确定
                queryTimePoint = targetTimePoint;
            }

            log.info("处理KPI引用: {}, hasExplicitTimeModifier={}, queryTimePoint={}",
                    fullRef, hasExplicitTimeModifier, queryTimePoint);

            // 根据时间修饰符获取对应的时间点
            String targetOpTime;
            switch (queryTimePoint) {
                case "current":
                    targetOpTime = currentOpTime;
                    break;
                case "lastCycle":
                    targetOpTime = lastCycleOpTime;
                    break;
                case "lastYear":
                    targetOpTime = lastYearOpTime;
                    break;
                default:
                    log.warn("未知的时间修饰符: {}，使用当前时间", queryTimePoint);
                    targetOpTime = currentOpTime;
            }

            // 使用和普通KPI一致的聚合方式：基于主表t聚合，使用case when表达式
            // 这确保 KD1002 和 ${KD1002} 返回相同结果
            // 注意：else分支使用NULL而不是'--'，避免字符串转换为数字0影响sum结果
            String replacement = String.format("sum(case when t.kpi_id = '%s' and t.op_time = '%s' then t.kpi_val else null end)",
                    ref.kpiId(), targetOpTime);
            String logMsg = String.format("转换KPI引用: %s -> %s, 原始引用: %s, 目标时间点: %s, 时间值: %s",
                    ref.kpiId(), replacement, ref.fullReference(), queryTimePoint, targetOpTime);
            log.info(logMsg);
            sqlExpression = sqlExpression.replace(ref.fullReference(), replacement);
        }

        log.info("完整表达式转换: 原始={}, 转换后={}", expression, sqlExpression);
        return sqlExpression;
    }

    /**
     * 构建包含维度描述字段的GROUP BY子句（用于单行查询）
     */
    private String buildGroupByClauseForSingleQuery(List<String> groupByFields) {
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

    @NotNull
    private String transformSql(String kpiExpr, String opTime) {
        Pattern pattern = Pattern.compile("\\b(K[DCYM]\\d{4})\\b");
        Matcher matcher = pattern.matcher(kpiExpr);

        StringBuilder sb = new StringBuilder();
        while (matcher.find()) {
            String kpiId = matcher.group(1);
            String replacement = String.format("sum(case when t.kpi_id = '%s' and t.op_time = '%s' then t.kpi_val else null end)", kpiId, opTime);
            matcher.appendReplacement(sb, replacement);
        }
        matcher.appendTail(sb);

        return sb.toString();
    }


    private String buildTargetJoinConditionForTarget(List<String> groupByFields, String opTime) {
        StringBuilder joinCondition = new StringBuilder();
        joinCondition.append(String.format("t.kpi_id = target.kpi_id and t.op_time = '%s'", opTime));

        for (String field : groupByFields) {
            joinCondition.append(String.format(" and t.%s = target.%s", field, field));
        }

        return joinCondition.toString();
    }


    /**
     * 从表达式中提取所有KPI ID
     */
    private Set<String> extractKpiIdsFromExpr(String kpiExpr) {
        Set<String> kpiIds = new HashSet<>();
        if (kpiExpr == null || kpiExpr.isEmpty()) {
            return kpiIds;
        }

        Pattern pattern = Pattern.compile("\\b(K[DCYM]\\d{4})\\b");
        Matcher matcher = pattern.matcher(kpiExpr);
        while (matcher.find()) {
            kpiIds.add(matcher.group(1));
        }

        return kpiIds;
    }


    /**
     * 默认RDB实现
     * @param kpiId
     * @param cycleType
     * @param compDimCode
     * @param opTime
     * @return
     */
    @Override
    protected String getKpiDataTableName(String kpiId, String cycleType, String compDimCode, String opTime) {
        return String.format("kpi_%s_%s", cycleType.toLowerCase(), compDimCode.toLowerCase()).intern();
    }

    @Override
    protected String getDimDataTableName(String compDimCode) {
        return String.format("kpi_dim_%s", compDimCode);
    }

    @Override
    protected KpiQueryResult tryGetFromCache(KpiQueryRequest request) {
        // 1. 尝试从缓存获取
        String cacheKey = generateCacheKey(request);
        KpiQueryResult cachedResult = getFromCache(cacheKey);

        if (cachedResult != null) {
            log.info("缓存命中: {}", cacheKey);
            return cachedResult;
        }

        log.info("缓存未命中，查询数据库: {}", cacheKey);
        return null;
    }

    @Override
    protected void postQuery(KpiQueryRequest request, List<Map<String, Object>> aggregatedResults) {
        // 根据配置过滤结果字段
        // 注意：这里的过滤是在聚合之后进行的
        boolean includeHistorical = request.includeHistoricalData() != null ? request.includeHistoricalData() : true;
        boolean includeTarget = request.includeTargetData() != null ? request.includeTargetData() : false;

        // 如果不需要过滤，直接返回
        if (includeHistorical && includeTarget) {
            return;
        }

        // 过滤掉不需要的字段
        for (Map<String, Object> row : aggregatedResults) {
            @SuppressWarnings("unchecked")
            Map<String, Map<String, Object>> kpiValues = (Map<String, Map<String, Object>>) row.get("kpiValues");

            for (Map<String, Object> values : kpiValues.values()) {
                if (!includeHistorical) {
                    values.remove("lastYear");
                    values.remove("lastCycle");
                }

                if (!includeTarget) {
                    values.remove("targetValue");
                    values.remove("checkResult");
                    values.remove("checkDesc");
                }
            }
        }
    }

    @Override
    protected void tryPutToCache(KpiQueryRequest request, List<Map<String, Object>> aggregatedResults) {
        // 构建缓存结果
        String cacheKey = generateCacheKey(request);
        KpiQueryResult cachedResult = KpiQueryResult.success(aggregatedResults, "查询成功");

        // 将结果存入缓存
        putToCache(cacheKey, cachedResult);
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

        // 添加历史数据和目标值的配置到缓存key
        // includeHistoricalData: true=包含历史, false=不包含历史
        boolean includeHistorical = request.includeHistoricalData() != null ? request.includeHistoricalData() : true;
        keyBuilder.append(":hist:").append(includeHistorical);

        // includeTargetData: true=包含目标值, false=不包含目标值
        boolean includeTarget = request.includeTargetData() != null ? request.includeTargetData() : false;
        keyBuilder.append(":target:").append(includeTarget);

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






    /**
     * 根据查询配置过滤结果字段
     * 注意：新结构中历史数据和目标值已经被正确地聚合到kpiValues内部
     * 这个方法现在主要用于检查是否需要过滤kpiValues中的某些字段
     */
    private List<Map<String, Object>> filterResultFields(
            List<Map<String, Object>> allResults,
            KpiQueryRequest request) {

        // 获取配置
        boolean includeHistorical = request.includeHistoricalData() != null ? request.includeHistoricalData() : true;
        boolean includeTarget = request.includeTargetData() != null ? request.includeTargetData() : false;

        // 如果两个都包含（默认情况），直接返回原结果
        if (includeHistorical && includeTarget) {
            return allResults;
        }

        // 创建新的结果列表
        List<Map<String, Object>> filteredResults = new ArrayList<>();

        for (Map<String, Object> row : allResults) {
            Map<String, Object> filteredRow = new LinkedHashMap<>(row);

            // 检查是否有kpiValues字段需要过滤
            if (row.containsKey("kpiValues")) {
                @SuppressWarnings("unchecked")
                Map<String, Map<String, Object>> kpiValues =
                        (Map<String, Map<String, Object>>) row.get("kpiValues");

                // 为每个KPI过滤字段
                for (Map.Entry<String, Map<String, Object>> kpiEntry : kpiValues.entrySet()) {
                    Map<String, Object> kpiValueMap = kpiEntry.getValue();

                    // 如果不包含历史数据，移除lastYear和lastCycle
                    if (!includeHistorical) {
                        kpiValueMap.remove("lastYear");
                        kpiValueMap.remove("lastCycle");
                    }

                    // 如果不包含目标值，移除相关字段
                    if (!includeTarget) {
                        kpiValueMap.remove("targetValue");
                        kpiValueMap.remove("checkResult");
                        kpiValueMap.remove("checkDesc");
                    }
                }

                filteredRow.put("kpiValues", kpiValues);
            }

            filteredResults.add(filteredRow);
        }

        return filteredResults;
    }


}
