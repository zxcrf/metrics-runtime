package com.asiainfo.metrics.v2.core.parser;

import com.asiainfo.metrics.v2.core.model.*;
import com.asiainfo.metrics.v2.infra.persistence.MetadataRepository;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 指标解析器 (Production Ready)
 * 增强特性：
 * 1. 循环依赖检测 (Cycle Detection)。
 * 2. 健壮的日期计算。
 * 3. 递归深度限制。
 */
@ApplicationScoped
public class MetricParser {

    private static final Logger log = LoggerFactory.getLogger(MetricParser.class);
    private static final int MAX_RECURSION_DEPTH = 50; // 防止过深栈溢出

    @Inject MetadataRepository metadataRepo;

    // 匹配 ${KD1001} 或 ${KD1001.lastYear}
    private static final Pattern VAR_PATTERN = Pattern.compile("\\$\\{([A-Z0-9_]+)(\\.([a-zA-Z]+))?\\}");
    private static final DateTimeFormatter DATE_FMT = DateTimeFormatter.ofPattern("yyyyMMdd");

    /**
     * 解析依赖 (入口)
     */
    public void resolveDependencies(MetricDefinition metric, String baseOpTime, String compDimCode, QueryContext ctx) {
        // 创建一个新的访问路径集合，用于检测循环依赖
        resolveRecursive(metric, baseOpTime, compDimCode, ctx, new HashSet<>(), 0);
    }

    /**
     * 递归解析核心逻辑
     */
    private void resolveRecursive(MetricDefinition metric, String currentOpTime, String compDimCode,
                                  QueryContext ctx, Set<String> visitedPath, int depth) {

        // 1. 深度防御
        if (depth > MAX_RECURSION_DEPTH) {
            throw new RuntimeException("Expression complexity exceeds limit (depth > " + MAX_RECURSION_DEPTH + "): " + metric.id());
        }

        // 2. 循环依赖检测
        // Path Key: ID + Time (因为 KD1001.current 依赖 KD1001.lastYear 不算循环，但 KD1001 依赖 KD1001 是循环)
        String pathKey = metric.id() + "@" + currentOpTime;
        if (visitedPath.contains(pathKey)) {
            throw new RuntimeException("Detected Circular Dependency: " + pathKey + " path: " + visitedPath);
        }
        visitedPath.add(pathKey);

        try {
            String expr = metric.expression();
            if (expr == null || expr.isEmpty()) return;

            Matcher matcher = VAR_PATTERN.matcher(expr);
            while (matcher.find()) {
                String refId = matcher.group(1);     // e.g., KD1001
                String modifier = matcher.group(3);  // e.g., lastYear

                // 3. 计算目标时间
                String targetTime = calculateTime(currentOpTime, modifier);

                // 4. 获取被引用指标的定义
                MetricDefinition refDef = metadataRepo.findById(refId);

                // 5. 分支处理
                if (refDef.type() == MetricType.PHYSICAL) {
                    // 叶子节点：注册物理表需求
                    ctx.addPhysicalTable(refId, targetTime, compDimCode);
                } else {
                    // 复合/虚拟节点：递归向下
                    // 注意：必须创建 visitedPath 的副本传给分支，或者在回溯时 remove (但在递归树中 copy Set 更安全简单)
                    Set<String> branchVisited = new HashSet<>(visitedPath);
                    resolveRecursive(refDef, targetTime, compDimCode, ctx, branchVisited, depth + 1);
                }
            }
        } finally {
            // 如果使用同一个 Set 对象并在回溯时 remove，需要在这里执行 visitedPath.remove(pathKey);
            // 但为了代码简单性，上面使用了 new HashSet<>(visitedPath) 传递给子级
        }
    }

    /**
     * 日期计算 (健壮版)
     */
    public String calculateTime(String baseTime, String modifier) {
        // 快速路径
        if (modifier == null || modifier.isEmpty() || "current".equals(modifier)) {
            return baseTime;
        }

        try {
            LocalDate date = LocalDate.parse(baseTime, DATE_FMT);

            return switch (modifier) {
                case "lastYear" -> date.minusYears(1).format(DATE_FMT);
                case "lastCycle", "lastMonth" -> date.minusMonths(1).format(DATE_FMT);
                case "lastDay" -> date.minusDays(1).format(DATE_FMT);
                default -> {
                    log.warn("Unknown time modifier: {}, ignoring.", modifier);
                    yield baseTime;
                }
            };
        } catch (DateTimeParseException e) {
            log.warn("Invalid date format for calculation: baseTime={}, modifier={}", baseTime, modifier);
            // 容错：如果日期解析失败，回退到原始时间，避免查询完全失败
            return baseTime;
        }
    }
}