package com.asiainfo.metrics.v2.core.generator;

import com.asiainfo.metrics.v2.core.MetricsConstants;
import com.asiainfo.metrics.v2.core.model.*;
import com.asiainfo.metrics.v2.core.parser.MetricParser;
import com.asiainfo.metrics.v2.infra.persistence.MetadataRepository;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import java.util.*;
import java.util.regex.Matcher;
import java.util.stream.Collectors;

@ApplicationScoped
public class SqlGenerator {

    @Inject MetadataRepository metadataRepo;
    @Inject MetricParser parser;

    public String generateSql(List<MetricDefinition> metrics, QueryContext ctx, List<String> dims) {
        return generateSqlInternal(metrics, ctx, dims, null);
    }

    public String generateSqlWithStaging(List<MetricDefinition> metrics, QueryContext ctx, List<String> dims, String stagingTableName) {
        return generateSqlInternal(metrics, ctx, dims, stagingTableName);
    }

    private String generateSqlInternal(List<MetricDefinition> metrics, QueryContext ctx, List<String> dims, String stagingTableName) {
        StringBuilder sql = new StringBuilder();

        // 【修复点1】构建带表别名的维度字段列表 (e.g., "raw_union.city_id, raw_union.county_id")
        // 这样 SQLite 就知道我们选的是主表的维度列
        String qualifiedDimFields = dims.stream()
                .map(d -> "raw_union." + d)
                .collect(Collectors.joining(", "));

        // 原始字段名，用于 UNION 子查询的构建
        String dimFields = String.join(", ", dims);

        // 1. 数据源 CTE
        if (stagingTableName != null) {
            sql.append("WITH raw_union AS (SELECT * FROM ").append(stagingTableName).append(")");
        } else {
            List<String> unions = ctx.getRequiredTables().stream()
                    .map(req -> generateUnionQuery(req, dimFields, ctx)) // 这里传原始字段名即可
                    .collect(Collectors.toList());
            if (unions.isEmpty()) return "";
            sql.append("WITH raw_union AS (\n").append(String.join("\nUNION ALL\n", unions)).append("\n)");
        }

        // 2. 目标值 CTE
        if (ctx.isIncludeTarget()) {
            sql.append(",\ntarget_values AS (\n");
            String mainCompDim = findMainCompDimCode(ctx);
            sql.append(generateTargetValuesQuery(ctx, dims, mainCompDim));
            sql.append("\n)");
        }

        // 3. 主查询
        sql.append("\nSELECT ");
        if (!dims.isEmpty()) {
            sql.append(qualifiedDimFields); // 【修复点2】使用带别名的字段
        }

        for (MetricDefinition metric : metrics) {
            // 处理逗号逻辑
            if (!dims.isEmpty() || metrics.indexOf(metric) > 0) {
                sql.append(",\n  ");
            } else {
                sql.append("\n  ");
            }
            String sqlExpr = transpileToSql(metric.expression(), ctx, metric.aggFunc(), dims);
            sql.append(sqlExpr).append(" AS ").append(metric.id());
        }

        // 目标值描述字段 (t.xxx_desc)
        if (ctx.isIncludeTarget()) {
            for (String dim : dims) sql.append(",\n  t.").append(dim).append("_desc");
        }

        sql.append("\nFROM raw_union");

        // 4. 维度 JOIN
        if (!dims.isEmpty()) {
            String mainCompDim = findMainCompDimCode(ctx);
            sql.append("\nLEFT JOIN (\n");
            sql.append(generateDimensionJoinQuery(ctx, dims, mainCompDim));
            sql.append("\n) t ON ").append(generateJoinCondition(dims));

            // 【修复点3】GROUP BY 也使用带别名的字段
            sql.append("\nGROUP BY ").append(qualifiedDimFields);
            // 添加维度描述字段到 GROUP BY
            sql.append(", ").append(dims.stream().map(d -> "t." + d + "_desc").collect(Collectors.joining(", ")));
        } else {
            // 如果没有维度，通常是聚合查询，不需要 GROUP BY 或者 GROUP BY 常量
            // 如果确实需要 group by dimFields (虽然为空)，保持一致性
            if (!dims.isEmpty()) {
                sql.append("\nGROUP BY ").append(qualifiedDimFields);
            }
        }

        return sql.toString();
    }

    // ... generateUnionQuery, findMainCompDimCode, generateDimensionJoinQuery,
    // ... generateTargetValuesQuery, generateJoinCondition, transpileToSql
    // ... 保持之前修正过的版本不变 ...

    // 为了完整性，这里再次列出 generateUnionQuery，确保它使用正确的 context
    private String generateUnionQuery(PhysicalTableReq req, String ignoredDimFields, QueryContext ctx) {
        String dbAlias = ctx.getAlias(req.kpiId(), req.opTime());
        String tableName = String.format("kpi_%s_%s_%s", req.kpiId(), req.opTime(), req.compDimCode());

        List<String> requestedDims = ctx.getDimCodes();
        Set<String> tableActualDims = metadataRepo.getDimCols(req.compDimCode());

        StringBuilder smartSelect = new StringBuilder();
        for (String dim : requestedDims) {
            if (tableActualDims.contains(dim)) {
                smartSelect.append(dim).append(", ");
            } else {
                smartSelect.append("NULL as ").append(dim).append(", ");
            }
        }

        if (smartSelect.length() > 0) {
            smartSelect.setLength(smartSelect.length() - 2);
            smartSelect.append(", ");
        }

        return String.format(
                "SELECT %s'%s' as kpi_id, '%s' as op_time, kpi_val FROM %s.%s",
                smartSelect.toString(), req.kpiId(), req.opTime(), dbAlias, tableName
        );
    }

    // ... 其他辅助方法保持不变 ...
    private String findMainCompDimCode(QueryContext ctx) {
        List<String> reqDims = ctx.getDimCodes();
        if (reqDims.isEmpty()) return "CD003";
        String bestCode = "CD003";
        long maxMatches = -1;
        Set<String> involvedCodes = ctx.getRequiredTables().stream()
                .map(PhysicalTableReq::compDimCode)
                .filter(Objects::nonNull)
                .collect(Collectors.toSet());
        for (String code : involvedCodes) {
            Set<String> tableDims = metadataRepo.getDimCols(code);
            long matches = reqDims.stream().filter(tableDims::contains).count();
            if (matches > maxMatches) {
                maxMatches = matches;
                bestCode = code;
            }
        }
        return bestCode;
    }

    private String generateDimensionJoinQuery(QueryContext ctx, List<String> dims, String compDimCode) {
        if (dims.isEmpty()) return "";
        StringBuilder sql = new StringBuilder();
        sql.append("SELECT DISTINCT ");
        List<String> selectFields = new ArrayList<>();
        for (String dim : dims) {
            selectFields.add(dim);
            selectFields.add(dim + "_desc");
        }
        sql.append(String.join(", ", selectFields));
        sql.append("\nFROM ").append(String.format("kpi_dim_%s", compDimCode));
        return sql.toString();
    }

    private String generateTargetValuesQuery(QueryContext ctx, List<String> dims, String compDimCode) {
        return "SELECT * FROM " + String.format("kpi_target_value_%s", compDimCode);
    }

    private String generateJoinCondition(List<String> dims) {
        return dims.stream().map(d -> "raw_union." + d + " = t." + d).collect(Collectors.joining(" AND "));
    }

    private String transpileToSql(String domainExpr, QueryContext ctx, String aggFunc, List<String> dims) {
        Matcher matcher = MetricsConstants.VARIABLE_PATTERN.matcher(domainExpr);
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
}