package com.asiainfo.metrics.v2.core.parser;

import com.asiainfo.metrics.v2.core.model.*;
import com.asiainfo.metrics.v2.infra.persistence.MetadataRepository;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@ApplicationScoped
public class MetricParser {

    private static final Logger log = LoggerFactory.getLogger(MetricParser.class);
    private static final Pattern VAR_PATTERN = Pattern.compile("\\$\\{([A-Z0-9_]+)(\\.([a-zA-Z]+))?\\}");
    private static final DateTimeFormatter DATE_FMT = DateTimeFormatter.ofPattern("yyyyMMdd");

    @Inject MetadataRepository metadataRepo;

    /**
     * 解析依赖 (API 变更：移除 compDimCode 参数)
     */
    public void resolveDependencies(MetricDefinition metric, String baseOpTime, QueryContext ctx) {
        resolveRecursive(metric, baseOpTime, ctx, new HashSet<>(), 0);
    }

    private void resolveRecursive(MetricDefinition metric, String currentOpTime,
                                  QueryContext ctx, Set<String> visitedPath, int depth) {
        if (depth > 50) throw new RuntimeException("Expression complexity exceeds limit: " + metric.id());

        // 如果是物理指标，直接注册
        if (metric.type() == MetricType.PHYSICAL) {
            // 关键点：直接使用 Metric 定义中的 compDimCode
            if (metric.compDimCode() == null) {
                log.warn("Physical metric {} has no compDimCode, using default CD003", metric.id());
                ctx.addPhysicalTable(metric.id(), currentOpTime, "CD003");
            } else {
                ctx.addPhysicalTable(metric.id(), currentOpTime, metric.compDimCode());
            }
            return; // 物理指标没有表达式需要解析
        }

        // 解析表达式中的依赖 (Virtual / Composite)
        String pathKey = metric.id() + "@" + currentOpTime;
        if (visitedPath.contains(pathKey)) throw new RuntimeException("Circular Dependency: " + pathKey);
        visitedPath.add(pathKey);

        try {
            String expr = metric.expression();
            if (expr == null || expr.isEmpty()) return;

            Matcher matcher = VAR_PATTERN.matcher(expr);
            while (matcher.find()) {
                String refId = matcher.group(1);
                String modifier = matcher.group(3);
                String targetTime = calculateTime(currentOpTime, modifier);

                // 递归查找依赖的指标定义
                MetricDefinition refDef = metadataRepo.findById(refId);

                // 递归调用 (Set copy for branch safety)
                resolveRecursive(refDef, targetTime, ctx, new HashSet<>(visitedPath), depth + 1);
            }
        } finally {
            // no-op
        }
    }

    // calculateTime 方法保持不变...
    public String calculateTime(String baseTime, String modifier) {
        if (modifier == null || modifier.isEmpty() || "current".equals(modifier)) return baseTime;
        try {
            LocalDate date = LocalDate.parse(baseTime, DATE_FMT);
            return switch (modifier) {
                case "lastYear" -> date.minusYears(1).format(DATE_FMT);
                case "lastCycle", "lastMonth" -> date.minusMonths(1).format(DATE_FMT);
                default -> baseTime;
            };
        } catch (Exception e) { return baseTime; }
    }
}