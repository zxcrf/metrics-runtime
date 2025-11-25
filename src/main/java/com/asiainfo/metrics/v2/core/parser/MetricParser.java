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
        if (depth > 50) throw new RuntimeException("Expression depth limit exceeded: " + metric.id());

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

        try {
            String expr = metric.expression();
            if (expr != null) {
                // DEBUG LOGGING START
                log.debug("Matching expression: '{}' with pattern: '{}'", expr, MetricsConstants.VARIABLE_PATTERN.pattern());
                // DEBUG LOGGING END

                Matcher matcher = MetricsConstants.VARIABLE_PATTERN.matcher(expr);
                while (matcher.find()) {
                    String refId = matcher.group(1);
                    log.debug("Found dependency: {}", refId); // DEBUG LOG

                    String modifier = matcher.group(3);
                    String targetTime = calculateTime(currentOpTime, modifier);

                    MetricDefinition refDef = metadataRepo.findById(refId);
                    if (refDef == null) {
                        log.error("Metric definition not found for: {}", refId);
                        throw new RuntimeException("Dependent metric not found: " + refId);
                    }
                    resolveRecursive(refDef, targetTime, ctx, new HashSet<>(visitedPath), depth + 1);
                }
            }
        } finally {
            // no-op
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
        } catch (Exception e) { return baseTime; }
    }
}