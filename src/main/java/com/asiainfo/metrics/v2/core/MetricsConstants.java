package com.asiainfo.metrics.v2.core;

import java.util.regex.Pattern;

public class MetricsConstants {
    public static final String KPI_ID_REGEX = "K[DCYM]\\d{4}";
    public static final Pattern VARIABLE_PATTERN = Pattern.compile("\\$\\{(" + KPI_ID_REGEX + ")(\\.([a-zA-Z]+))?\\}");
    private MetricsConstants() {}
}