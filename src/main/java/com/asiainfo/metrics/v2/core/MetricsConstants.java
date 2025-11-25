package com.asiainfo.metrics.v2.core;

import java.util.regex.Pattern;

public class MetricsConstants {
    // 严格匹配 K 开头，第二位 D/C/Y/M，后跟4位数字
    public static final String KPI_ID_REGEX = "K[DCYM]\\d{4}";

    // 匹配 ${KD1001} 或 ${KD1001.lastYear}
    public static final Pattern VARIABLE_PATTERN = Pattern.compile("\\$\\{(" + KPI_ID_REGEX + ")(\\.([a-zA-Z]+))?\\}");

    private MetricsConstants() {}
}