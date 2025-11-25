package com.asiainfo.metrics.v2.core.generator;

import com.asiainfo.metrics.v2.core.model.MetricDefinition;
import com.asiainfo.metrics.v2.core.model.PhysicalTableReq;
import com.asiainfo.metrics.v2.core.model.QueryContext;
import com.asiainfo.metrics.v2.core.parser.MetricParser;
import com.asiainfo.metrics.v2.infra.persistence.MetadataRepository;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * SQL生成器
 * 负责将指标定义和查询上下文转换为最终的SQL查询
 */
@ApplicationScoped
public class SqlGenerator {

    @Inject
    MetricParser parser;

    @Inject
    MetadataRepository metadataRepository;

    // 匹配 ${KPI_ID} 或 ${KPI_ID.modifier} 格式的正则表达式
    private static final Pattern VARIABLE_PATTERN = Pattern.compile("\\$\\{([A-Z0-9]+)(\\.([a-zA-Z]+))?\\}");

    /**
     * 生成完整的SQL查询
     *
     * @param metrics 要查询的指标列表
     * @param ctx 查询上下文
     * @param dims 维度字段列表，如 ["city_id", "county_id"]
     * @return 完整的SQL查询字符串
     */
    public String generateSql(List<MetricDefinition> metrics, QueryContext ctx, List<String> dims) {
        StringBuilder sql = new StringBuilder();

        // 设置维度代码到上下文
        for (String dim : dims) {
            ctx.addDimCode(dim);
        }

        // 构建维度字段列表
        String dimFields = String.join(", ", dims);

        // 1. 生成CTE (WITH子查询)
        sql.append("WITH raw_union AS (\n");
        List<String> unions = ctx.getRequiredTables().stream()
                .map(req -> generateUnionQuery(req, dimFields, ctx))
                .collect(Collectors.toList());

        if (unions.isEmpty()) {
            return "";
        }

        sql.append(String.join("\nUNION ALL\n", unions));
        sql.append("\n)");

        // 2. 如果包含目标值，生成目标值CTE
        if (ctx.isIncludeTarget()) {
            sql.append(",\ntarget_values AS (\n");
            sql.append(generateTargetValuesQuery(ctx, dims));
            sql.append("\n)");
        }

        // 3. 生成主查询
        sql.append("\nSELECT ").append(dimFields);

        for (MetricDefinition metric : metrics) {
            validateExpression(metric.expression());

            sql.append(",\n  ");
            String sqlExpr = transpileToSql(metric.expression(), ctx, metric.aggFunc(), dims);
            sql.append(sqlExpr).append(" AS ").append(metric.id());
        }

        // 添加目标值列
        if (ctx.isIncludeTarget()) {
            for (String dim : dims) {
                sql.append(",\n  t.").append(dim).append("_desc");
            }
        }

        sql.append("\nFROM raw_union");

        // JOIN维度表（只有当维度表存在时才添加）
        if (!dims.isEmpty() && hasDimensionTable(ctx)) {
            sql.append("\nLEFT JOIN (\n");
            sql.append(generateDimensionJoinQuery(ctx, dims));
            sql.append("\n) t ON ");
            sql.append(generateJoinCondition(dims));

            // GROUP BY 需要包含维度描述字段
            sql.append("\nGROUP BY ").append(dimFields);
            sql.append(", ");
            sql.append(dims.stream().map(d -> "t." + d + "_desc").collect(Collectors.joining(", ")));
        } else {
            // 没有维度表，只按维度字段分组
            sql.append("\nGROUP BY ").append(dimFields);
        }

        return sql.toString();
    }

    private void validateExpression(String expr) {
        // 仅允许：字母数字(KPI ID)、大括号、运算符、空格、小数点、数字
        if (!expr.matches("^[\\w\\s\\+\\-\\*/\\(\\)\\.\\$\\{\\}]+$")) {
            throw new SecurityException("非法指标表达式: " + expr);
        }
    }

    /**
     * 生成UNION子查询
     *
     * @param req 物理表请求
     * @param dimFields 维度字段
     * @param ctx 查询上下文
     * @return UNION子查询字符串
     */
    private String generateUnionQuery(PhysicalTableReq req, String dimFields, QueryContext ctx) {
        String dbAlias = ctx.getAlias(req.kpiId(), req.opTime());
        String tableName = req.toTableName();

        // 构建UNION查询，从ATTACH的数据库中查询数据
        return String.format(
            "SELECT %s, '%s' as kpi_id, '%s' as op_time, kpi_val " +
            "FROM %s.%s",
            dimFields, req.kpiId(), req.opTime(), dbAlias, tableName
        );
    }

    /**
     * 将指标表达式转换为SQL表达式
     *
     * @param domainExpr 领域表达式，如 ${KD1002} + ${KD1005}
     * @param ctx 查询上下文
     * @param aggFunc 聚合函数
     * @param dims 维度字段列表
     * @return SQL表达式
     */
    private String transpileToSql(String domainExpr, QueryContext ctx, String aggFunc, List<String> dims) {
        Matcher matcher = VARIABLE_PATTERN.matcher(domainExpr);
        StringBuilder sb = new StringBuilder();

        while (matcher.find()) {
            String kpiId = matcher.group(1);
            String modifier = matcher.group(3);

            // 计算目标时间
            String targetOpTime = parser.calculateTime(ctx.getOpTime(), modifier);

            // 构建聚合表达式
            String aggPart = String.format(
                "%s(CASE WHEN kpi_id='%s' AND op_time='%s' THEN kpi_val ELSE NULL END)",
                aggFunc != null ? aggFunc : "sum",
                kpiId,
                targetOpTime
            );

            matcher.appendReplacement(sb, aggPart);
        }
        matcher.appendTail(sb);

        return sb.toString();
    }

    /**
     * 生成维度表JOIN查询
     *
     * @param ctx 查询上下文
     * @param dims 维度字段列表
     * @return 维度JOIN查询SQL
     */
    private String generateDimensionJoinQuery(QueryContext ctx, List<String> dims) {
        if (dims.isEmpty()) {
            return "";
        }

        StringBuilder sql = new StringBuilder();
        sql.append("SELECT DISTINCT ");

        // 选择维度字段和描述字段
        List<String> selectFields = new ArrayList<>();
        for (String dim : dims) {
            selectFields.add(dim);
            selectFields.add(dim + "_desc");
        }
        sql.append(String.join(", ", selectFields));

        sql.append("\nFROM ").append(generateDimTableName(ctx));

        return sql.toString();
    }

    /**
     * 生成JOIN条件
     *
     * @param dims 维度字段列表
     * @return JOIN条件字符串
     */
    private String generateJoinCondition(List<String> dims) {
        return dims.stream()
                .map(dim -> "raw_union." + dim + " = t." + dim)
                .collect(Collectors.joining(" AND "));
    }

    /**
     * 生成目标值查询
     *
     * @param ctx 查询上下文
     * @param dims 维度字段列表
     * @return 目标值查询SQL
     */
    private String generateTargetValuesQuery(QueryContext ctx, List<String> dims) {
        // 这里应该从目标值表查询，目前返回空实现
        // 实际实现中需要从metadataRepository获取目标值表信息
        StringBuilder sql = new StringBuilder();
        sql.append("SELECT ");

        List<String> selectFields = new ArrayList<>();
        selectFields.addAll(dims);
        for (String dim : dims) {
            selectFields.add(dim + "_desc");
        }
        sql.append(String.join(", ", selectFields));
        sql.append("\nFROM ").append(generateTargetTableName(ctx));
        sql.append("\nWHERE op_time = '").append(ctx.getOpTime()).append("'");

        return sql.toString();
    }

    /**
     * 生成维度表名
     *
     * @param ctx 查询上下文
     * @return 维度表名
     */
    private String generateDimTableName(QueryContext ctx) {
        return String.format("kpi_dim_%s", ctx.getCompDimCode());
    }

    /**
     * 生成目标值表名
     *
     * @param ctx 查询上下文
     * @return 目标值表名
     */
    private String generateTargetTableName(QueryContext ctx) {
        return String.format("kpi_target_value_%s", ctx.getCompDimCode());
    }

    /**
     * 检查是否存在维度表
     * 目前简化为检查includeTarget或includeHistorical标志
     * 实际实现中应该从元数据仓库查询维度表是否存在
     *
     * @param ctx 查询上下文
     * @return 是否存在维度表
     */
    private boolean hasDimensionTable(QueryContext ctx) {
        // 如果包含目标值或历史数据，才需要维度表
        // 否则跳过维度JOIN以避免表不存在错误
        return ctx.isIncludeTarget() || ctx.isIncludeHistorical();
    }
}
