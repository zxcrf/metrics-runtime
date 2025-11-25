package com.asiainfo.metrics.v2;

import com.asiainfo.metrics.v2.core.model.QueryContext;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * QueryContext增强功能测试
 */
public class QueryContextTest {

    @Test
    public void testDimensionManagement() {
        System.out.println("\n╔══════════════════════════════════════════════════════════════════════════════╗");
        System.out.println("║                    测试: QueryContext维度管理                               ║");
        System.out.println("╚══════════════════════════════════════════════════════════════════════════════╝");

        QueryContext ctx = new QueryContext();

        // 添加维度代码
        ctx.addDimCode("city_id");
        ctx.addDimCode("county_id");
        ctx.addDimCode("province_id");

        List<String> dimCodes = ctx.getDimCodes();
        assertEquals(3, dimCodes.size());
        assertTrue(dimCodes.contains("city_id"));
        assertTrue(dimCodes.contains("county_id"));
        assertTrue(dimCodes.contains("province_id"));

        System.out.println("维度代码列表: " + dimCodes);
        System.out.println("✓ 维度管理功能正常");
        System.out.println("══════════════════════════════════════════════════════════════════════════════\n");
    }

    @Test
    public void testHistoricalTimeCalculations() {
        System.out.println("\n╔══════════════════════════════════════════════════════════════════════════════╗");
        System.out.println("║                  测试: 历史时间计算（去年、上期）                           ║");
        System.out.println("╚══════════════════════════════════════════════════════════════════════════════╝");

        QueryContext ctx = new QueryContext();
        ctx.setOpTime("20251024");

        // 计算去年
        String lastYear = ctx.getLastYearTime();
        assertEquals("20241024", lastYear);
        System.out.println("20251024 -> 去年: " + lastYear);

        // 计算上期
        String lastCycle = ctx.getLastCycleTime();
        assertEquals("20250924", lastCycle);
        System.out.println("20251024 -> 上期: " + lastCycle);

        // 测试边界情况：1月
        ctx.setOpTime("20250124");
        String lastCycleJan = ctx.getLastCycleTime();
        assertEquals("20241224", lastCycleJan);
        System.out.println("20250124 -> 上期: " + lastCycleJan + " (跨年)");

        // 测试去年（跨年）
        String lastYearJan = ctx.getLastYearTime();
        assertEquals("20240124", lastYearJan);
        System.out.println("20250124 -> 去年: " + lastYearJan);

        System.out.println("✓ 历史时间计算正确");
        System.out.println("══════════════════════════════════════════════════════════════════════════════\n");
    }

    @Test
    public void testFlags() {
        System.out.println("\n╔══════════════════════════════════════════════════════════════════════════════╗");
        System.out.println("║                      测试: 标志位（历史数据、目标值）                       ║");
        System.out.println("╚══════════════════════════════════════════════════════════════════════════════╝");

        QueryContext ctx = new QueryContext();

        // 默认值
        assertFalse(ctx.isIncludeHistorical());
        assertFalse(ctx.isIncludeTarget());

        // 设置历史数据标志
        ctx.setIncludeHistorical(true);
        assertTrue(ctx.isIncludeHistorical());

        // 设置目标值标志
        ctx.setIncludeTarget(true);
        assertTrue(ctx.isIncludeTarget());

        System.out.println("历史数据标志: " + ctx.isIncludeHistorical());
        System.out.println("目标值标志: " + ctx.isIncludeTarget());
        System.out.println("✓ 标志位功能正常");
        System.out.println("══════════════════════════════════════════════════════════════════════════════\n");
    }

    @Test
    public void testClear() {
        System.out.println("\n╔══════════════════════════════════════════════════════════════════════════════╗");
        System.out.println("║                        测试: 清空上下文                                     ║");
        System.out.println("╚══════════════════════════════════════════════════════════════════════════════╝");

        QueryContext ctx = new QueryContext();

        // 添加一些数据
        ctx.setOpTime("20251024");
        ctx.setCompDimCode("CD003");
        ctx.addDimCode("city_id");
        ctx.setIncludeHistorical(true);
        ctx.setIncludeTarget(true);
        ctx.addPhysicalTable("KD1002", "20251024", "CD003");

        // 验证有数据
        assertNotNull(ctx.getOpTime());
        assertNotNull(ctx.getCompDimCode());
        assertFalse(ctx.getDimCodes().isEmpty());
        assertTrue(ctx.isIncludeHistorical());
        assertTrue(ctx.isIncludeTarget());
        assertFalse(ctx.getRequiredTables().isEmpty());

        System.out.println("清空前:");
        System.out.println("  OpTime: " + ctx.getOpTime());
        System.out.println("  CompDimCode: " + ctx.getCompDimCode());
        System.out.println("  维度数量: " + ctx.getDimCodes().size());
        System.out.println("  历史数据: " + ctx.isIncludeHistorical());
        System.out.println("  目标值: " + ctx.isIncludeTarget());
        System.out.println("  物理表数量: " + ctx.getRequiredTables().size());

        // 清空
        ctx.clear();

        // 验证清空
        assertNull(ctx.getOpTime());
        assertNull(ctx.getCompDimCode());
        assertTrue(ctx.getDimCodes().isEmpty());
        assertFalse(ctx.isIncludeHistorical());
        assertFalse(ctx.isIncludeTarget());
        assertTrue(ctx.getRequiredTables().isEmpty());

        System.out.println("\n清空后:");
        System.out.println("  OpTime: " + ctx.getOpTime());
        System.out.println("  CompDimCode: " + ctx.getCompDimCode());
        System.out.println("  维度数量: " + ctx.getDimCodes().size());
        System.out.println("  历史数据: " + ctx.isIncludeHistorical());
        System.out.println("  目标值: " + ctx.isIncludeTarget());
        System.out.println("  物理表数量: " + ctx.getRequiredTables().size());

        System.out.println("✓ 清空功能正常");
        System.out.println("══════════════════════════════════════════════════════════════════════════════\n");
    }
}
