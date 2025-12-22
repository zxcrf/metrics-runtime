package com.asiainfo.metrics.domain.generator;

import com.asiainfo.metrics.shared.MetricsConstants;
import com.asiainfo.metrics.domain.model.*;
import com.asiainfo.metrics.domain.parser.MetricParser;
import com.asiainfo.metrics.infrastructure.persistence.MetadataRepository;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.regex.Matcher;
import java.util.stream.Collectors;

@ApplicationScoped
public class SqlGenerator {

    private static final Logger log = LoggerFactory.getLogger(SqlGenerator.class);

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
        // 跳过缺失的表（下载失败的）
        Map<String, List<String>> compDimFiles = new HashMap<>();

        for (PhysicalTableReq req : ctx.getRequiredTables()) {
            String path = ctx.getAlias(req.kpiId(), req.opTime());
            if (path == null) {
                // 缺失的表，跳过
                log.debug("[SQL Gen] Skipping missing table: kpi={}, opTime={}", req.kpiId(), req.opTime());
                continue;
            }
            compDimFiles.computeIfAbsent(req.compDimCode(), k -> new ArrayList<>()).add("'" + path + "'");
        }

        sql.append("WITH raw_union AS (\n");
        List<String> cteParts = new ArrayList<>();

        log.debug("[SQL Gen] Required tables count: {}, compDimFiles: {}",
                ctx.getRequiredTables().size(), compDimFiles.size());

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

        if (cteParts.isEmpty())
            return "";

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
            subSql.append("'").append(reqTime).append("' as opTime");

            for (MetricDefinition metric : metrics) {
                subSql.append(",\n  ");
                String sqlExpr;
                // log.info("Processing metric {} with type={}, expr={}", metric.id(), metric.type(), metric.expression());
                
                // 累计指标：直接在这里处理，展开日期范围
                if (metric.type() == MetricType.CUMULATIVE) {
                    String sourceKpiId = metric.expression(); // expression 存储源指标ID
                    List<String> dateRange = parser.expandToMonthStart(reqTime);
                    String dateList = dateRange.stream().map(d -> "'" + d + "'").collect(Collectors.joining(","));
                    // log.info("Generating cumulative SQL for {}: source={}, dates={}", metric.id(), sourceKpiId, dateRange);
                    sqlExpr = String.format(
                            "%s(CASE WHEN kpi_id='%s' AND op_time IN (%s) THEN kpi_val ELSE NULL END)",
                            metric.aggFunc() != null ? metric.aggFunc() : "sum", sourceKpiId, dateList);
                } else {
                    sqlExpr = transpileToSqlForBatch(metric.expression(), reqTime, metric.aggFunc());
                }
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

            // WHERE - 维度过滤条件
            if (ctx.hasDimConditions()) {
                List<String> whereClauses = new ArrayList<>();
                for (Map.Entry<String, List<String>> entry : ctx.getDimConditions().entrySet()) {
                    String dimCode = entry.getKey();
                    List<String> values = entry.getValue();
                    if (values.size() == 1) {
                        // 单值用 = 
                        whereClauses.add(String.format("raw_union.%s = '%s'", dimCode, values.get(0)));
                    } else {
                        // 多值用 IN
                        String inList = values.stream()
                                .map(v -> "'" + v + "'")
                                .collect(Collectors.joining(", "));
                        whereClauses.add(String.format("raw_union.%s IN (%s)", dimCode, inList));
                    }
                }
                subSql.append("\n WHERE ").append(String.join(" AND ", whereClauses));
            }

            // GROUP BY - only if we have dimensions
            if (!dims.isEmpty()) {
                subSql.append("\n GROUP BY ");
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

    private String generateSqlInternal(List<MetricDefinition> metrics, QueryContext ctx, List<String> dims,
            String stagingTableName) {
        // ... (保持旧代码不变，为了节省空间此处省略) ...
        return "";
    }

    private String generateUnionQuery(PhysicalTableReq req, String ignoredDimFields, QueryContext ctx) {
        // ... (保持旧代码不变) ...
        return "";
    }

    private String findMainCompDimCode(QueryContext ctx) {
        List<String> reqDims = ctx.getDimCodes();
        if (reqDims.isEmpty())
            return "CD003";
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

    /**
     * 将领域表达式转换为 SQL 聚合表达式
     * 支持两种格式：
     * 1. 完整格式：${KD1001.current}
     * 2. 简化格式：KD1001 (自动转换为 ${KD1001.current})
     */
    private String transpileToSqlForBatch(String domainExpr, String currentOpTime, String aggFunc) {
        // 预处理：将简化格式转换为完整格式
        String normalizedExpr = normalizeExpression(domainExpr);

        // 使用正则表达式匹配 ${KPI_ID.modifier} 格式
        Matcher matcher = MetricsConstants.VARIABLE_PATTERN.matcher(normalizedExpr);
        StringBuilder sb = new StringBuilder();
        while (matcher.find()) {
            String kpiId = matcher.group(1);
            String modifier = matcher.group(3);
            String targetOpTime = parser.calculateTime(currentOpTime, modifier);

            // 检查是否是复合指标，如果是则递归展开
            MetricDefinition refDef = metadataRepo.findById(kpiId);
            String replacement;

            if (refDef != null && refDef.type() == com.asiainfo.metrics.domain.model.MetricType.COMPOSITE) {
                // 复合指标：递归展开其表达式
                log.debug("Expanding composite metric in SQL: {} -> {}", kpiId, refDef.expression());
                replacement = transpileToSqlForBatch(refDef.expression(), targetOpTime, refDef.aggFunc());
            } else if (refDef != null && refDef.type() == com.asiainfo.metrics.domain.model.MetricType.CUMULATIVE) {
                // 累计指标：展开日期范围（月初到目标日期）并生成 IN 子句
                // 特殊处理：累计指标的 lastCycle 应该是上月同一天，而不是昨天
                String effectiveTargetTime = targetOpTime;
                if ("lastCycle".equals(modifier)) {
                    // 累计指标的 lastCycle = 上月同一天
                    effectiveTargetTime = parser.calculateTime(currentOpTime, "lastMonth");
                    log.debug("Cumulative lastCycle adjusted: {} -> {}", targetOpTime, effectiveTargetTime);
                }
                
                String sourceKpiId = refDef.expression();
                List<String> dateRange = parser.expandToMonthStart(effectiveTargetTime);
                String dateList = dateRange.stream().map(d -> "'" + d + "'").collect(Collectors.joining(","));
                log.debug("Expanding cumulative metric in SQL: {} -> source={}, dates={}", kpiId, sourceKpiId, dateRange.size());
                replacement = String.format(
                        "%s(CASE WHEN kpi_id='%s' AND op_time IN (%s) THEN kpi_val ELSE NULL END)",
                        refDef.aggFunc() != null ? refDef.aggFunc() : "sum", sourceKpiId, dateList);
            } else {
                // 物理指标：直接生成 CASE WHEN
                replacement = String.format(
                        "%s(CASE WHEN kpi_id='%s' AND op_time='%s' THEN kpi_val ELSE NULL END)",
                        aggFunc != null ? aggFunc : "sum", kpiId, targetOpTime);
            }

            matcher.appendReplacement(sb, Matcher.quoteReplacement(replacement));
        }
        matcher.appendTail(sb);
        return sb.toString();
    }

    /**
     * 规范化表达式：将简化格式转换为完整格式
     * 例如：KD1001 + KD1002 → ${KD1001.current} + ${KD1002.current}
     */
    private String normalizeExpression(String expr) {
        if (expr == null || expr.isEmpty()) {
            return expr;
        }

        // 匹配不在 ${} 中的指标ID
        // \b(K[DCYM]\d{4})\b 匹配完整的指标ID
        // (?<!\$\{) 负向后查找：前面不是 ${
        // (?!\.) 负向前查找：后面不是 .
        String pattern = "(?<!\\$\\{)\\b(K[DCYM]\\d{4})\\b(?!\\.)";
        String result = expr.replaceAll(pattern, "\\${$1.current}");

        if (!expr.equals(result)) {
            org.slf4j.LoggerFactory.getLogger(SqlGenerator.class)
                    .debug("Expression normalized: [{}] -> [{}]", expr, result);
        }

        return result;
    }

    private String transpileToSql(String domainExpr, QueryContext ctx, String aggFunc, List<String> dims) {
        return transpileToSqlForBatch(domainExpr, ctx.getOpTime(), aggFunc);
    }
}