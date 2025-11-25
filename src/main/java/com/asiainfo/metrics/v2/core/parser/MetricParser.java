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
     * 入口方法变更：不再需要传入 compDimCode
     */
    public void resolveDependencies(MetricDefinition metric, String baseOpTime, QueryContext ctx) {
        resolveRecursive(metric, baseOpTime, ctx, new HashSet<>(), 0);
    }

    private void resolveRecursive(MetricDefinition metric, String currentOpTime,
                                  QueryContext ctx, Set<String> visitedPath, int depth) {
        if (depth > 50) throw new RuntimeException("Expression depth limit exceeded: " + metric.id());

        // 1. 如果是物理指标，直接注册需求
        if (metric.type() == MetricType.PHYSICAL) {
            // 核心修复：直接使用指标元数据中定义的 compDimCode
            // 这样即使 Virtual KPI 没有维度，它依赖的物理指标也能找到自己的家
            String targetCompDim = metric.compDimCode();
            if (targetCompDim == null) {
                log.warn("Physical metric {} missing compDimCode", metric.id());
                throw new RuntimeException("Physical metric missing compDimCode");
            }
            ctx.addPhysicalTable(metric.id(), currentOpTime, targetCompDim);
            return;
        }

        // 2. 如果是虚拟/复合指标，递归解析表达式
        String pathKey = metric.id() + "@" + currentOpTime;
        if (visitedPath.contains(pathKey)) throw new RuntimeException("Circular dependency detected: " + pathKey);
        visitedPath.add(pathKey);

        try {
            String expr = metric.expression();
            if (expr != null) {
                Matcher matcher = VAR_PATTERN.matcher(expr);
                while (matcher.find()) {
                    String refId = matcher.group(1);
                    String modifier = matcher.group(3);
                    String targetTime = calculateTime(currentOpTime, modifier);

                    MetricDefinition refDef = metadataRepo.findById(refId);
                    // 递归向下，不再传递任何维度信息，完全由下层指标自己决定
                    resolveRecursive(refDef, targetTime, ctx, new HashSet<>(visitedPath), depth + 1);
                }
            }
        } finally {
            // path clean up if needed
        }
    }

    public String calculateTime(String baseTime, String modifier) {
        if (modifier == null || "current".equals(modifier)) return baseTime;
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