package com.asiainfo.metrics.v2;

import com.asiainfo.metrics.v2.core.MetricsConstants;
import org.junit.jupiter.api.Test;
import java.util.regex.Matcher;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class RegexDebug {

    @Test
    public void debugRegex() {
        String input = "${KD1002}";
        System.out.println("Testing input: " + input);
        System.out.println("Using Pattern: " + MetricsConstants.VARIABLE_PATTERN.pattern());
        System.out.println("Using KPI Regex: " + MetricsConstants.KPI_ID_REGEX);

        Matcher matcher = MetricsConstants.VARIABLE_PATTERN.matcher(input);
        boolean found = matcher.find();

        System.out.println("Match found? " + found);

        if (found) {
            System.out.println("Group 1 (ID): " + matcher.group(1));
        } else {
            System.err.println("❌ 正则匹配失败！请检查 MetricsConstants 中的转义字符。");
            // 诊断建议
            if (MetricsConstants.KPI_ID_REGEX.contains("\\\\d")) {
                System.err.println("发现 KPI_ID_REGEX 中可能有双重转义 (\\\\d)，请改为单转义 (\\d)。");
            }
        }
        assertTrue(found, "Regex failed to match standard KPI format");
    }
}