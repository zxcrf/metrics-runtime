package com.asiainfo.metrics.v2.core.parser;

import com.asiainfo.metrics.v2.core.MetricsConstants;
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

@ApplicationScoped
public class MetricParser {

    private static final Logger log = LoggerFactory.getLogger(MetricParser.class);
    private static final DateTimeFormatter DATE_FMT = DateTimeFormatter.ofPattern("yyyyMMdd");

    @Inject MetadataRepository metadataRepo;

    public void resolveDependencies(MetricDefinition metric, String baseOpTime, QueryContext ctx) {
        resolveRecursive(metric, baseOpTime, ctx, new HashSet<>(), 0);
    }

    private void resolveRecursive(MetricDefinition metric, String currentOpTime,
                                  QueryContext ctx, Set<String> visitedPath, int depth) {
        // 1. 深度防御
        if (depth > 50) {
            throw new RuntimeException("Expression depth limit exceeded: " + metric.id());
        }

        String pathKey = metric.id() + "@" + currentOpTime;
        log.debug("Resolving dependency: {} (Depth: {})", pathKey, depth);

        // 2. 循环依赖检测
        if (visitedPath.contains(pathKey)) {
            String msg = "Circular dependency detected: " + pathKey + " in path " + visitedPath;
            log.error(msg);
            throw new RuntimeException(msg);
        }
        visitedPath.add(pathKey);

        // 3. 物理指标处理（递归终点）
        if (metric.type() == MetricType.PHYSICAL) {
            String targetCompDim = metric.compDimCode();
            if (targetCompDim == null) {
                log.warn("Physical metric {} missing compDimCode, defaulting to CD003", metric.id());
                throw new RuntimeException("Physical metric missing compDimCode: " + metric.id());
            }
            ctx.addPhysicalTable(metric.id(), currentOpTime, targetCompDim);
            return;
        }

        // 4. 复合/虚拟指标解析
        try {
            String expr = metric.expression();
            if (expr != null) {
                // 使用统一常量
                Matcher matcher = MetricsConstants.VARIABLE_PATTERN.matcher(expr);
                while (matcher.find()) {
                    String refId = matcher.group(1);
                    String modifier = matcher.group(3);
                    String targetTime = calculateTime(currentOpTime, modifier);

                    // 关键：加载依赖的指标定义
                    MetricDefinition refDef = metadataRepo.findById(refId);
                    if (refDef == null) {
                        throw new RuntimeException("Dependent metric not found: " + refId);
                    }

                    // 关键：传递 visitedPath 的副本给分支，保证路径隔离
                    // (虽然这里用副本，但在检测直系循环依赖时，当前的 visitedPath 已经包含了自身)
                    resolveRecursive(refDef, targetTime, ctx, new HashSet<>(visitedPath), depth + 1);
                }
            }
        } finally {
            // 这里的 finally 不需要 remove，因为我们传给子级的是 HashSet 的副本
        }
    }

    public String calculateTime(String baseTime, String modifier) {
        if (modifier == null || modifier.isEmpty() || "current".equals(modifier)) return baseTime;
        try {
            LocalDate date = LocalDate.parse(baseTime, DATE_FMT);
            return switch (modifier) {
                case "lastYear" -> date.minusYears(1).format(DATE_FMT);
                case "lastCycle", "lastMonth" -> date.minusMonths(1).format(DATE_FMT);
                default -> baseTime;
            };
        } catch (Exception e) {
            log.warn("Date parse failed for {}, returning baseTime", baseTime);
            return baseTime;
        }
    }
}