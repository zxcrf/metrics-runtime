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

        // 维度字段处理：明确指定从 raw_union 获取 ID
        String qualifiedDimFields = dims.stream()
                .map(d -> "raw_union." + d)
                .collect(Collectors.joining(", "));
        String dimFields = String.join(", ", dims); // 用于子查询生成

        // 1. 数据源 CTE
        if (stagingTableName != null) {
            sql.append("WITH raw_union AS (SELECT * FROM ").append(stagingTableName).append(")");
        } else {
            List<String> unions = ctx.getRequiredTables().stream()
                    .map(req -> generateUnionQuery(req, dimFields, ctx))
                    .collect(Collectors.toList());
            if (unions.isEmpty()) return "";
            sql.append("WITH raw_union AS (\n").append(String.join("\nUNION ALL\n", unions)).append("\n)");
        }

        // 2. 目标值 CTE (保持原有逻辑，如果不需要可以移除)
        if (ctx.isIncludeTarget()) {
            sql.append(",\ntarget_values AS (\n");
            String mainCompDim = findMainCompDimCode(ctx);
            sql.append(generateTargetValuesQuery(ctx, dims, mainCompDim));
            sql.append("\n)");
        }

        // 3. 主查询 SELECT
        sql.append("\nSELECT ");
        if (!dims.isEmpty()) {
            sql.append(qualifiedDimFields);
        }

        for (MetricDefinition metric : metrics) {
            if (!dims.isEmpty() || metrics.indexOf(metric) > 0) {
                sql.append(",\n  ");
            } else {
                sql.append("\n  ");
            }
            String sqlExpr = transpileToSql(metric.expression(), ctx, metric.aggFunc(), dims);
            sql.append(sqlExpr).append(" AS ").append(metric.id());
        }

        // 添加维度描述字段 (从 JOIN 的维度表中获取)
        if (!dims.isEmpty()) {
            for (String dim : dims) {
                // t_city_id.dim_val as city_id_desc
                sql.append(",\n  t_").append(dim).append(".dim_val as ").append(dim).append("_desc");
            }
        }

        sql.append("\nFROM raw_union");

        // 4. 维度 JOIN (核心修复：纵表多次 JOIN)
        if (!dims.isEmpty()) {
            String compDimCode = findMainCompDimCode(ctx);
            String dimTableName = String.format("kpi_dim_%s", compDimCode);

            for (String dim : dims) {
                String alias = "t_" + dim; // 为每个维度创建一个别名表，如 t_city_id

                // LEFT JOIN kpi_dim_CD003 t_city_id
                // ON raw_union.city_id = t_city_id.dim_code AND t_city_id.dim_id = 'city_id'
                sql.append("\nLEFT JOIN ").append(dimTableName).append(" ").append(alias);
                sql.append(" ON raw_union.").append(dim).append(" = ").append(alias).append(".dim_code");
                sql.append(" AND ").append(alias).append(".dim_id = '").append(dim).append("'");
            }

            // GROUP BY
            sql.append("\nGROUP BY ").append(qualifiedDimFields);
            for (String dim : dims) {
                sql.append(", ").append("t_").append(dim).append(".dim_val");
            }
        } else {
            // 无维度聚合
            if (!qualifiedDimFields.isEmpty()) {
                sql.append("\nGROUP BY ").append(qualifiedDimFields);
            }
        }

        return sql.toString();
    }

    // 智能 UNION: 物理表缺少的维度列补 NULL
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

    // ... 辅助方法保持不变 ...
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