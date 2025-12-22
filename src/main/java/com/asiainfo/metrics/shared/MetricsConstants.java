package com.asiainfo.metrics.shared;

import java.util.regex.Pattern;

/**
 * 指标系统常量
 */
public final class MetricsConstants {

    private MetricsConstants() {
        // Utility class
    }

    /**
     * 指标变量表达式匹配正则
     * 匹配格式: ${KPI_ID.modifier}
     * 例如: ${KD1001.current}, ${KD1002.lastYear}
     * Group 1: KPI ID
     * Group 3: modifier (optional)
     */
    public static final Pattern VARIABLE_PATTERN = Pattern.compile(
            "\\$\\{([A-Z]{2}\\d{4})(\\.(\\w+))?\\}"
    );
}
