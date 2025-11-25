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

    private static final Pattern VARIABLE_PATTERN = Pattern.compile("\\$\\{([A-Z0-9]+)(\\.([a-zA-Z]+))?\\}");

    /**
     * 生成完整的SQL查询 (标准模式 - 适用于少量表)
     */
    public String generateSql(List<MetricDefinition> metrics, QueryContext ctx, List<String> dims) {
        return generateSqlInternal(metrics, ctx, dims, null);
    }

    /**
     * 生成完整的SQL查询 (暂存表模式 - 适用于大量表)
     *
     * @param stagingTableName 预加载好数据的表名
     */
    public String generateSqlWithStaging(List<MetricDefinition> metrics, QueryContext ctx, List<String> dims, String stagingTableName) {
        return generateSqlInternal(metrics, ctx, dims, stagingTableName);
    }

    private String generateSqlInternal(List<MetricDefinition> metrics, QueryContext ctx, List<String> dims, String stagingTableName) {
        StringBuilder sql = new StringBuilder();

        // 设置维度代码到上下文
        for (String dim : dims) {
            ctx.addDimCode(dim);
        }
        String dimFields = String.join(", ", dims);

        // 1. 数据源准备
        if (stagingTableName != null) {
            // 模式B: 使用预加载的暂存表
            // 为了保持后续逻辑一致，我们定义一个名为 raw_union 的 CTE 指向暂存表
            sql.append("WITH raw_union AS (SELECT * FROM ").append(stagingTableName).append(")");
        } else {
            // 模式A: 生成 UNION ALL CTE
            List<String> unions = ctx.getRequiredTables().stream()
                    .map(req -> generateUnionQuery(req, dimFields, ctx))
                    .collect(Collectors.toList());

            if (unions.isEmpty()) return ""; // 无数据源

            sql.append("WITH raw_union AS (\n");
            sql.append(String.join("\nUNION ALL\n", unions));
            sql.append("\n)");
        }

        // 2. 目标值CTE (如果需要)
        if (ctx.isIncludeTarget()) {
            sql.append(",\ntarget_values AS (\n");
            sql.append(generateTargetValuesQuery(ctx, dims));
            sql.append("\n)");
        }

        // 3. 生成主查询
        sql.append("\nSELECT ").append(dimFields);

        for (MetricDefinition metric : metrics) {
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

        // JOIN维度表
        if (!dims.isEmpty() && hasDimensionTable(ctx)) {
            sql.append("\nLEFT JOIN (\n");
            sql.append(generateDimensionJoinQuery(ctx, dims));
            sql.append("\n) t ON ");
            sql.append(generateJoinCondition(dims));

            sql.append("\nGROUP BY ").append(dimFields);
            sql.append(", ");
            sql.append(dims.stream().map(d -> "t." + d + "_desc").collect(Collectors.joining(", ")));
        } else {
            sql.append("\nGROUP BY ").append(dimFields);
        }

        return sql.toString();
    }

    private String generateUnionQuery(PhysicalTableReq req, String dimFields, QueryContext ctx) {
        String dbAlias = ctx.getAlias(req.kpiId(), req.opTime());
        String tableName = req.toTableName();
        return String.format(
                "SELECT %s, '%s' as kpi_id, '%s' as op_time, kpi_val FROM %s.%s",
                dimFields, req.kpiId(), req.opTime(), dbAlias, tableName
        );
    }

    private String transpileToSql(String domainExpr, QueryContext ctx, String aggFunc, List<String> dims) {
        Matcher matcher = VARIABLE_PATTERN.matcher(domainExpr);
        StringBuilder sb = new StringBuilder();

        while (matcher.find()) {
            String kpiId = matcher.group(1);
            String modifier = matcher.group(3);
            String targetOpTime = parser.calculateTime(ctx.getOpTime(), modifier);

            String aggPart = String.format(
                    "%s(CASE WHEN kpi_id='%s' AND op_time='%s' THEN kpi_val ELSE NULL END)",
                    aggFunc != null ? aggFunc : "sum", kpiId, targetOpTime
            );
            matcher.appendReplacement(sb, aggPart);
        }
        matcher.appendTail(sb);
        return sb.toString();
    }

    // ... (保留 generateDimensionJoinQuery, generateJoinCondition, generateTargetValuesQuery 等辅助方法不变)

    private String generateDimensionJoinQuery(QueryContext ctx, List<String> dims) {
        if (dims.isEmpty()) return "";
        StringBuilder sql = new StringBuilder();
        sql.append("SELECT DISTINCT ");
        List<String> selectFields = new ArrayList<>();
        for (String dim : dims) {
            selectFields.add(dim);
            selectFields.add(dim + "_desc");
        }
        sql.append(String.join(", ", selectFields));
        sql.append("\nFROM ").append(generateDimTableName(ctx));
        return sql.toString();
    }

    private String generateJoinCondition(List<String> dims) {
        return dims.stream()
                .map(dim -> "raw_union." + dim + " = t." + dim)
                .collect(Collectors.joining(" AND "));
    }

    private String generateTargetValuesQuery(QueryContext ctx, List<String> dims) {
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

    private String generateDimTableName(QueryContext ctx) {
        return String.format("kpi_dim_%s", ctx.getCompDimCode());
    }

    private String generateTargetTableName(QueryContext ctx) {
        return String.format("kpi_target_value_%s", ctx.getCompDimCode());
    }

    private boolean hasDimensionTable(QueryContext ctx) {
        return ctx.isIncludeTarget() || ctx.isIncludeHistorical();
    }
}