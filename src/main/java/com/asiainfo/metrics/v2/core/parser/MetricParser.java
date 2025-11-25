package com.asiainfo.metrics.v2.core.parser;

import com.asiainfo.metrics.v2.core.model.*;
import com.asiainfo.metrics.v2.infra.persistence.MetadataRepository;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@ApplicationScoped
public class MetricParser {

    @Inject MetadataRepository metadataRepo;

    // 匹配 ${KD1001} 或 ${KD1001.lastYear}
    private static final Pattern VAR_PATTERN = Pattern.compile("\\$\\{([A-Z0-9_]+)(\\.([a-zA-Z]+))?\\}");
    private static final DateTimeFormatter DATE_FMT = DateTimeFormatter.ofPattern("yyyyMMdd");

    /**
     * 递归解析指标依赖
     */
    public void resolveDependencies(MetricDefinition metric, String baseOpTime, String compDimCode, QueryContext ctx) {
        String expr = metric.expression();
        if (expr == null) return;

        Matcher matcher = VAR_PATTERN.matcher(expr);
        while (matcher.find()) {
            String refId = matcher.group(1);     // KD1001
            String modifier = matcher.group(3);  // lastYear (可能为null)

            // 1. 计算该变量对应的物理时间
            String targetTime = calculateTime(baseOpTime, modifier);

            // 2. 检查引用的指标是物理的还是复合/虚拟的
            // 如果是 KD 开头，查元数据
            MetricDefinition refDef = metadataRepo.findById(refId);

            if (refDef == null || refDef.type() == MetricType.PHYSICAL) {
                // 叶子节点：直接注册物理表需求
                ctx.addPhysicalTable(refId, targetTime, compDimCode);
            } else {
                // 非物理节点（复合指标）：递归解析
                // 注意：这里需要把计算后的 targetTime 作为新的 baseTime 传递下去
                resolveDependencies(refDef, targetTime, compDimCode, ctx);
            }
        }
    }

    public String calculateTime(String baseTime, String modifier) {
        if (modifier == null || "current".equals(modifier)) return baseTime;
        try {
            LocalDate date = LocalDate.parse(baseTime, DATE_FMT);
            if ("lastYear".equals(modifier)) {
                return date.minusYears(1).format(DATE_FMT);
            } else if ("lastCycle".equals(modifier)) {
                return date.minusMonths(1).format(DATE_FMT);
            }
        } catch (Exception e) {
            // ignore
        }
        return baseTime;
    }
}