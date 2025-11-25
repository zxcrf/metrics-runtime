package com.asiainfo.metrics.v2;

import com.asiainfo.metrics.v2.core.model.DimensionDef;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * DimensionDef功能测试
 */
public class DimensionDefTest {

    @Test
    public void testDimensionDefCreation() {
        System.out.println("\n╔══════════════════════════════════════════════════════════════════════════════╗");
        System.out.println("║                      测试: DimensionDef创建                                 ║");
        System.out.println("╚══════════════════════════════════════════════════════════════════════════════╝");

        DimensionDef dim = new DimensionDef(
            "city_id",
            "城市ID",
            "city_id",
            "kpi_dim_city"
        );

        assertEquals("city_id", dim.dimCode());
        assertEquals("城市ID", dim.dimName());
        assertEquals("city_id", dim.dbColName());
        assertEquals("kpi_dim_city", dim.dimTableName());

        System.out.println("维度代码: " + dim.dimCode());
        System.out.println("维度名称: " + dim.dimName());
        System.out.println("数据库列名: " + dim.dbColName());
        System.out.println("维度表名: " + dim.dimTableName());
        System.out.println("✓ DimensionDef创建正常");
        System.out.println("══════════════════════════════════════════════════════════════════════════════\n");
    }

    @Test
    public void testDimTableName() {
        System.out.println("\n╔══════════════════════════════════════════════════════════════════════════════╗");
        System.out.println("║                    测试: 生成维度表名                                       ║");
        System.out.println("╚══════════════════════════════════════════════════════════════════════════════╝");

        DimensionDef dim = new DimensionDef(
            "city_id",
            "城市ID",
            "city_id",
            "kpi_dim_city"
        );

        // 生成维度表名
        String tableName = dim.toDimTableName("CD003");
        assertEquals("kpi_dim_CD003", tableName);
        System.out.println("复合维度CD003的维度表名: " + tableName);

        // 测试其他复合维度
        String tableName2 = dim.toDimTableName("CD001");
        assertEquals("kpi_dim_CD001", tableName2);
        System.out.println("复合维度CD001的维度表名: " + tableName2);

        System.out.println("✓ 维度表名生成正确");
        System.out.println("══════════════════════════════════════════════════════════════════════════════\n");
    }

    @Test
    public void testDimValAlias() {
        System.out.println("\n╔══════════════════════════════════════════════════════════════════════════════╗");
        System.out.println("║                    测试: 生成维度值别名                                     ║");
        System.out.println("╚══════════════════════════════════════════════════════════════════════════════╝");

        // 测试city_id
        DimensionDef dim1 = new DimensionDef("city_id", "城市ID", "city_id", "kpi_dim_city");
        String alias1 = dim1.toDimValAlias();
        assertEquals("city_id_desc", alias1);
        System.out.println("city_id -> " + alias1);

        // 测试county_id
        DimensionDef dim2 = new DimensionDef("county_id", "区县ID", "county_id", "kpi_dim_county");
        String alias2 = dim2.toDimValAlias();
        assertEquals("county_id_desc", alias2);
        System.out.println("county_id -> " + alias2);

        // 测试province_id
        DimensionDef dim3 = new DimensionDef("province_id", "省份ID", "province_id", "kpi_dim_province");
        String alias3 = dim3.toDimValAlias();
        assertEquals("province_id_desc", alias3);
        System.out.println("province_id -> " + alias3);

        System.out.println("✓ 维度值别名生成正确");
        System.out.println("══════════════════════════════════════════════════════════════════════════════\n");
    }

    @Test
    public void testMultipleDimensions() {
        System.out.println("\n╔══════════════════════════════════════════════════════════════════════════════╗");
        System.out.println("║                    测试: 多维度定义                                         ║");
        System.out.println("╚══════════════════════════════════════════════════════════════════════════════╝");

        // 定义多个维度
        DimensionDef[] dims = {
            new DimensionDef("province_id", "省份ID", "province_id", "kpi_dim_province"),
            new DimensionDef("city_id", "城市ID", "city_id", "kpi_dim_city"),
            new DimensionDef("county_id", "区县ID", "county_id", "kpi_dim_county")
        };

        System.out.println("复合维度表: kpi_dim_CD003");
        System.out.println("\n维度列表:");
        for (DimensionDef dim : dims) {
            System.out.println("  - " + dim.dimCode() + " (" + dim.dimName() + ")");
            System.out.println("    数据库列: " + dim.dbColName());
            System.out.println("    描述别名: " + dim.toDimValAlias());
            System.out.println("    维度表: " + dim.toDimTableName("CD003"));
        }

        assertEquals(3, dims.length);
        System.out.println("✓ 多维度定义正确");
        System.out.println("══════════════════════════════════════════════════════════════════════════════\n");
    }
}
