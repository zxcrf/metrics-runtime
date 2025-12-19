package com.asiainfo.metrics.v2.domain.parser;

import com.asiainfo.metrics.v2.shared.MetricsConstants;
import com.asiainfo.metrics.v2.domain.model.*;
import com.asiainfo.metrics.v2.infrastructure.persistence.MetadataRepository;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;

@ApplicationScoped
public class MetricParser {

    private static final Logger log = LoggerFactory.getLogger(MetricParser.class);
    private static final DateTimeFormatter DATE_FMT = DateTimeFormatter.ofPattern("yyyyMMdd");

    @Inject
    MetadataRepository metadataRepo;

    public void resolveDependencies(MetricDefinition metric, String baseOpTime, QueryContext ctx) {
        resolveRecursive(metric, baseOpTime, ctx, new HashSet<>(), 0);
    }

    private void resolveRecursive(MetricDefinition metric, String currentOpTime,
            QueryContext ctx, Set<String> visitedPath, int depth) {
        if (depth > 50)
            throw new RuntimeException("Expression depth limit exceeded: " + metric.id());

        String pathKey = metric.id() + "@" + currentOpTime;
        log.debug("Resolving dependency: {} (Depth: {})", pathKey, depth);

        if (visitedPath.contains(pathKey)) {
            String msg = "Circular dependency detected: " + pathKey + " in path " + visitedPath;
            log.error(msg);
            throw new RuntimeException(msg);
        }
        visitedPath.add(pathKey);

        if (metric.type() == MetricType.PHYSICAL) {
            String targetCompDim = metric.compDimCode();
            if (targetCompDim == null) {
                targetCompDim = "CD003";
            }
            ctx.addPhysicalTable(metric.id(), currentOpTime, targetCompDim);
            return;
        }

        // 累计指标：展开日期范围（月初到目标日期）
        if (metric.type() == MetricType.CUMULATIVE) {
            String sourceKpiId = metric.expression(); // expression 存储源指标ID
            MetricDefinition sourceDef = metadataRepo.findById(sourceKpiId);
            if (sourceDef == null) {
                log.error("Source metric not found for cumulative: {}", sourceKpiId);
                throw new RuntimeException("Source metric not found: " + sourceKpiId);
            }
            List<String> dateRange = expandToMonthStart(currentOpTime);
            log.debug("Cumulative metric {} expanding dates: {} -> {}", metric.id(), currentOpTime, dateRange);
            for (String date : dateRange) {
                resolveRecursive(sourceDef, date, ctx, new HashSet<>(visitedPath), depth + 1);
            }
            return;
        }

        try {
            String expr = metric.expression();
            if (expr != null) {
                // 规范化表达式：支持简化格式 (KD1001 -> ${KD1001.current})
                String normalizedExpr = normalizeExpression(expr);

                // DEBUG LOGGING START
                if (!expr.equals(normalizedExpr)) {
                    log.debug("Expression normalized in dependency resolution: '{}' -> '{}'", expr, normalizedExpr);
                }
                log.debug("Matching expression: '{}' with pattern: '{}'", normalizedExpr,
                        MetricsConstants.VARIABLE_PATTERN.pattern());
                // DEBUG LOGGING END

                Matcher matcher = MetricsConstants.VARIABLE_PATTERN.matcher(normalizedExpr);
                while (matcher.find()) {
                    String refId = matcher.group(1);
                    log.debug("Found dependency: {}", refId); // DEBUG LOG

                    String modifier = matcher.group(3);
                    
                    MetricDefinition refDef = metadataRepo.findById(refId);
                    if (refDef == null) {
                        log.error("Metric definition not found for: {}", refId);
                        throw new RuntimeException("Dependent metric not found: " + refId);
                    }
                    
                    // 特殊处理：累计指标的 lastCycle 应该是上月同一天
                    String effectiveModifier = modifier;
                    if ("lastCycle".equals(modifier) && refDef.type() == MetricType.CUMULATIVE) {
                        effectiveModifier = "lastMonth";
                        log.debug("Cumulative lastCycle adjusted for dependency: {} lastCycle -> lastMonth", refId);
                    }
                    
                    String targetTime = calculateTime(currentOpTime, effectiveModifier);
                    resolveRecursive(refDef, targetTime, ctx, new HashSet<>(visitedPath), depth + 1);
                }
            }
        } finally {
            // no-op
        }
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
        return expr.replaceAll(pattern, "\\${$1.current}");
    }

    /**
     * 计算时间修饰符对应的时间点
     * 支持根据时间粒度自动判断周期：
     * - 日周期（YYYYMMDD 8位）: lastCycle = -1天
     * - 月周期（YYYYMM 6位）: lastCycle = -1月
     */
    public String calculateTime(String baseTime, String modifier) {
        if (modifier == null || modifier.isEmpty() || "current".equals(modifier)) {
            return baseTime;
        }

        try {
            // 根据时间格式判断粒度
            boolean isDailyGranularity = baseTime != null && baseTime.length() == 8;

            LocalDate date = LocalDate.parse(baseTime, DATE_FMT);
            return switch (modifier) {
                case "lastYear" -> date.minusYears(1).format(DATE_FMT);
                case "lastCycle" -> {
                    // 日周期：lastCycle = 昨天
                    // 月周期：lastCycle = 上月
                    if (isDailyGranularity) {
                        yield date.minusDays(1).format(DATE_FMT);
                    } else {
                        yield date.minusMonths(1).format(DATE_FMT);
                    }
                }
                case "lastMonth" -> date.minusMonths(1).format(DATE_FMT);
                default -> baseTime;
            };
        } catch (Exception e) {
            return baseTime;
        }
    }

    /**
     * 展开日期范围：从当月1日到目标日期
     * 例如：20251205 -> [20251201, 20251202, 20251203, 20251204, 20251205]
     */
    public List<String> expandToMonthStart(String opTime) {
        LocalDate targetDate = LocalDate.parse(opTime, DATE_FMT);
        LocalDate monthStart = targetDate.withDayOfMonth(1);
        List<String> dates = new ArrayList<>();
        for (LocalDate d = monthStart; !d.isAfter(targetDate); d = d.plusDays(1)) {
            dates.add(d.format(DATE_FMT));
        }
        return dates;
    }
}