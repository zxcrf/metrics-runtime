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

    @Inject
    MetadataRepository metadataRepo;
    @Inject
    MetricParser parser;

    // 保留旧方法用于兼容
    public String generateSql(QueryContext ctx) {
        return generateSqlInternal(ctx.getMetrics(), ctx, ctx.getDimCodes(), null);
    }

    public String generateSqlWithStaging(String stagingTableName, QueryContext ctx) {
        return generateSqlInternal(ctx.getMetrics(), ctx, ctx.getDimCodes(), stagingTableName);
    }

    /**
     * 生成批处理 SQL (New for DuckDB V2)
     * 使用 read_parquet list 模式替代大量的 UNION ALL
     */
    public String generateBatchSql(QueryContext ctx) {
        List<MetricDefinition> metrics = ctx.getMetrics();
        List<String> dims = ctx.getDimCodes();

        StringBuilder sql = new StringBuilder();

        // 1. 构建 CTE: 将所有相同组合维度的文件路径聚合
        Map<String, List<String>> compDimFiles = new HashMap<>();

        for (PhysicalTableReq req : ctx.getRequiredTables()) {
            String path = ctx.getAlias(req.kpiId(), req.opTime());
            compDimFiles.computeIfAbsent(req.compDimCode(), k -> new ArrayList<>()).add("'" + path + "'");
        }

        sql.append("WITH raw_union AS (\n");
        List<String> cteParts = new ArrayList<>();

        for (Map.Entry<String, List<String>> entry : compDimFiles.entrySet()) {
            String compDimCode = entry.getKey();
            List<String> filePaths = entry.getValue();
            String fileListStr = "[" + String.join(", ", filePaths) + "]";

            Set<String> actualDims = metadataRepo.getDimCols(compDimCode);

            StringBuilder selectPart = new StringBuilder();
            // 【关键修改】：显式转换 kpi_val 为 DOUBLE，兼容旧数据并确保类型安全
            // 如果文件里已经是 DOUBLE，CAST 是零开销；如果是 VARCHAR，会自动解析
            selectPart.append("kpi_id, op_time, CAST(kpi_val AS DOUBLE) as kpi_val");

            for (String dim : dims) {
                if (actualDims.contains(dim)) {
                    selectPart.append(", ").append(dim);
                } else {
                    selectPart.append(", NULL as ").append(dim);
                }
            }

            cteParts.add(String.format("SELECT %s FROM read_parquet(%s)", selectPart.toString(), fileListStr));
        }

        if (cteParts.isEmpty()) return "";

        sql.append(String.join("\n UNION ALL \n", cteParts));
        sql.append("\n)");

        // 2. Target Values CTE (Optional)
        if (ctx.isIncludeTarget()) {
            sql.append(",\ntarget_values AS (\n");
            String mainCompDim = findMainCompDimCode(ctx);
            sql.append("SELECT * FROM ").append(String.format("kpi_target_value_%s", mainCompDim));
            sql.append("\n)");
        }

        // 3. Main Select
        String qualifiedDimFields = dims.stream().map(d -> "raw_union." + d).collect(Collectors.joining(", "));

        // 4. 生成针对每个 Request Time 的聚合子查询
        List<String> timeSubQueries = new ArrayList<>();
        for (String reqTime : ctx.getTargetOpTimes()) {
            StringBuilder subSql = new StringBuilder();
            subSql.append("SELECT ");
            if (!dims.isEmpty()) {
                subSql.append(qualifiedDimFields).append(", ");
            }
            subSql.append("'").append(reqTime).append("' as op_time");

            for (MetricDefinition metric : metrics) {
                subSql.append(",\n  ");
                String sqlExpr = transpileToSqlForBatch(metric.expression(), reqTime, metric.aggFunc());
                subSql.append(sqlExpr).append(" AS ").append(metric.id());
            }

            // Dimension Descriptions
            if (!dims.isEmpty()) {
                for (String dim : dims) {
                    subSql.append(",\n  t_").append(dim).append(".dim_val as ").append(dim).append("_desc");
                }
            }

            subSql.append("\n FROM raw_union");

            // Dim Joins
            if (!dims.isEmpty()) {
                String compDimCode = findMainCompDimCode(ctx);
                String dimPath = ctx.getDimensionTablePaths().get(compDimCode);
                if (dimPath != null) {
                    String dimTableName = String.format("read_parquet('%s')", dimPath);

                    for (String dim : dims) {
                        String alias = "t_" + dim;
                        subSql.append("\n LEFT JOIN ").append(dimTableName).append(" ").append(alias);
                        subSql.append(" ON raw_union.").append(dim).append(" = ").append(alias).append(".dim_code");
                        subSql.append(" AND ").append(alias).append(".dim_id = '").append(dim).append("'");
                    }
                }
            }

            subSql.append("\n GROUP BY ");
            if (!dims.isEmpty()) {
                subSql.append(qualifiedDimFields);
                for (String dim : dims) {
                    subSql.append(", ").append("t_").append(dim).append(".dim_val");
                }
            }

            timeSubQueries.add(subSql.toString());
        }

        sql.append("\n");
        sql.append(String.join("\n UNION ALL \n", timeSubQueries));

        return sql.toString();
    }

    private String generateSqlInternal(List<MetricDefinition> metrics, QueryContext ctx, List<String> dims, String stagingTableName) {
        // ... (保持旧代码不变，为了节省空间此处省略) ...
        return "";
    }

    private String generateUnionQuery(PhysicalTableReq req, String ignoredDimFields, QueryContext ctx) {
        // ... (保持旧代码不变) ...
        return "";
    }

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

    private String transpileToSqlForBatch(String domainExpr, String currentOpTime, String aggFunc) {
        Matcher matcher = MetricsConstants.VARIABLE_PATTERN.matcher(domainExpr);
        StringBuilder sb = new StringBuilder();
        while (matcher.find()) {
            String kpiId = matcher.group(1);
            String modifier = matcher.group(3);
            String targetOpTime = parser.calculateTime(currentOpTime, modifier);

            String aggPart = String.format(
                    "%s(CASE WHEN kpi_id='%s' AND op_time='%s' THEN kpi_val ELSE NULL END)",
                    aggFunc != null ? aggFunc : "sum", kpiId, targetOpTime);
            matcher.appendReplacement(sb, aggPart);
        }
        matcher.appendTail(sb);
        return sb.toString();
    }

    private String transpileToSql(String domainExpr, QueryContext ctx, String aggFunc, List<String> dims) {
        return transpileToSqlForBatch(domainExpr, ctx.getOpTime(), aggFunc);
    }
}