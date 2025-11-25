package com.asiainfo.metrics.v2.core.generator;

import com.asiainfo.metrics.v2.core.model.*;
import com.asiainfo.metrics.v2.core.parser.MetricParser;
import com.asiainfo.metrics.v2.infra.persistence.MetadataRepository;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

@ApplicationScoped
public class SqlGenerator {

    @Inject MetricParser parser;
    @Inject MetadataRepository metadataRepository;

    private static final Pattern VARIABLE_PATTERN = Pattern.compile("\\$\\{([A-Z0-9]+)(\\.([a-zA-Z]+))?\\}");

    public String generateSql(List<MetricDefinition> metrics, QueryContext ctx, List<String> dims) {
        return generateSqlInternal(metrics, ctx, dims, null);
    }

    public String generateSqlWithStaging(List<MetricDefinition> metrics, QueryContext ctx, List<String> dims, String stagingTableName) {
        return generateSqlInternal(metrics, ctx, dims, stagingTableName);
    }

    private String generateSqlInternal(List<MetricDefinition> metrics, QueryContext ctx, List<String> dims, String stagingTableName) {
        StringBuilder sql = new StringBuilder();
        String dimFields = String.join(", ", dims);

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

        // 2. 目标值 CTE (如果需要)
        if (ctx.isIncludeTarget()) {
            sql.append(",\ntarget_values AS (\n");
            // 修复：根据实际涉及的 compDimCode 生成目标值查询
            // 简单起见，这里取第一个找到的 compDimCode，或者联合多个
            // 工程化建议：如果跨维度，应该分别 join。这里演示取“主”维度
            String mainCompDim = findMainCompDimCode(ctx);
            sql.append(generateTargetValuesQuery(ctx, dims, mainCompDim));
            sql.append("\n)");
        }

        // 3. 主查询
        sql.append("\nSELECT ").append(dimFields);
        for (MetricDefinition metric : metrics) {
            sql.append(",\n  ");
            String sqlExpr = transpileToSql(metric.expression(), ctx, metric.aggFunc(), dims);
            sql.append(sqlExpr).append(" AS ").append(metric.id());
        }

        // 目标值列
        if (ctx.isIncludeTarget()) {
            for (String dim : dims) sql.append(",\n  t.").append(dim).append("_desc");
        }

        sql.append("\nFROM raw_union");

        // 4. 维度 JOIN
        if (!dims.isEmpty()) {
            // 修复：动态确定要 JOIN 哪个维度表
            // 策略：收集所有涉及的 compDimCode，去重。
            // 实际上 SQLite 内存中我们可能需要 attach 维度表。
            // 简单策略：使用涉及到的第一个 compDimCode 作为维度描述来源
            String mainCompDim = findMainCompDimCode(ctx);

            // 注意：维度表必须在 executor 中被 attach 进来
            // 我们在 preparePhysicalTables 中其实只处理了数据表。
            // 还需要在 Engine 或 Executor 中处理维度表的加载。
            // 这里假设维度表名为 kpi_dim_{code}

            sql.append("\nLEFT JOIN (\n");
            sql.append(generateDimensionJoinQuery(ctx, dims, mainCompDim));
            sql.append("\n) t ON ").append(generateJoinCondition(dims));

            // Group By ...
            sql.append("\nGROUP BY ").append(dimFields);
            sql.append(", ").append(dims.stream().map(d -> "t." + d + "_desc").collect(Collectors.joining(", ")));
        } else {
            sql.append("\nGROUP BY ").append(dimFields);
        }

        return sql.toString();
    }

    // 辅助：找到一个“主”维度编码用于获取描述和目标值
    private String findMainCompDimCode(QueryContext ctx) {
        List<String> reqDims = ctx.getDimCodes();
        if (reqDims.isEmpty()) return "CD003";

        String bestCompDim = null;
        long maxMatchCount = -1;

        // 遍历所有涉及的 compDimCode
        Set<String> candidateCodes = ctx.getRequiredTables().stream()
                .map(PhysicalTableReq::compDimCode)
                .filter(Objects::nonNull)
                .collect(Collectors.toSet());

        for (String code : candidateCodes) {
            Set<String> tableDims = metadataRepository.getDimCols(code);
            // 计算匹配度：该表包含了多少个请求的维度
            long matchCount = reqDims.stream().filter(tableDims::contains).count();

            if (matchCount > maxMatchCount) {
                maxMatchCount = matchCount;
                bestCompDim = code;
            }
            // 如果找到了包含所有维度的表，直接返回
            if (matchCount == reqDims.size()) {
                return code;
            }
        }

        return bestCompDim != null ? bestCompDim : "CD003";
    }

    private String generateUnionQuery(PhysicalTableReq req, String dimFields, QueryContext ctx) {
        String dbAlias = ctx.getAlias(req.kpiId(), req.opTime());
        String tableName = String.format("kpi_%s_%s_%s", req.kpiId(), req.opTime(), req.compDimCode());

        // 获取该物理表实际拥有的维度列
        Set<String> availableDims = metadataRepository.getDimCols(req.compDimCode());
        List<String> reqDims = ctx.getDimCodes(); // 用户请求的维度列表

        // 构建 SELECT 列表
        StringBuilder selectClause = new StringBuilder();
        for (String reqDim : reqDims) {
            if (availableDims.contains(reqDim)) {
                selectClause.append(reqDim).append(", ");
            } else {
                // 关键修复：如果表里没有这个维度，填 NULL
                selectClause.append("NULL as ").append(reqDim).append(", ");
            }
        }

        // 去掉最后的逗号
        if (selectClause.length() > 0) {
            selectClause.setLength(selectClause.length() - 2);
        } else {
            // 如果没有维度，这部分为空，逻辑继续
        }

        // 拼接最终 SQL
        String dimSelect = selectClause.length() > 0 ? selectClause.toString() + ", " : "";

        return String.format(
                "SELECT %s'%s' as kpi_id, '%s' as op_time, kpi_val FROM %s.%s",
                dimSelect, req.kpiId(), req.opTime(), dbAlias, tableName
        );
    }

    // ... transpileToSql (保持不变) ...

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
        // 使用传入的 compDimCode
        sql.append("\nFROM ").append(String.format("kpi_dim_%s", compDimCode));
        return sql.toString();
    }

    // ... generateTargetValuesQuery 类似，增加 compDimCode 参数 ...
    private String generateTargetValuesQuery(QueryContext ctx, List<String> dims, String compDimCode) {
        // ... 类似 generateDimensionJoinQuery，使用 kpi_target_value_{compDimCode}
        return "SELECT ... FROM " + String.format("kpi_target_value_%s", compDimCode) + " ...";
    }

    // ... generateJoinCondition 保持不变 ...
    private String generateJoinCondition(List<String> dims) {
        return dims.stream().map(d -> "raw_union." + d + " = t." + d).collect(Collectors.joining(" AND "));
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
}