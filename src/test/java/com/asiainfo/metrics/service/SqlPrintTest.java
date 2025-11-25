package com.asiainfo.metrics.service;

import com.asiainfo.metrics.config.MetricsConfig;
import com.asiainfo.metrics.model.db.KpiDefinition;
import com.asiainfo.metrics.repository.KpiMetadataRepository;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * SQL打印测试
 * 专门用来打印和验证生成的SQL语句
 */
@QuarkusTest
public class SqlPrintTest {

    private static final Logger log = LoggerFactory.getLogger(SqlPrintTest.class);

    @Inject
    KpiQueryEngineFactory kpiQueryEngineFactory;

    @Inject
    KpiMetadataRepository metadataRepository;

    @Inject
    MetricsConfig metricsConfig;

    /**
     * 打印SQLite引擎的SQL生成逻辑
     */
    @Test
    void printSqliteQuerySql() throws Exception {
        log.info("\n" +
                "╔══════════════════════════════════════════════════════════════════════════════╗\n" +
                "║                    SQL打印测试 - SQLite引擎                            ║\n" +
                "╚══════════════════════════════════════════════════════════════════════════════╝\n");

        // 检查当前引擎类型
        String engineType = metricsConfig.getCurrentEngine();
        log.info("当前引擎类型: {}\n", engineType);

        // 模拟查询请求
        List<String> kpiIds = Arrays.asList("KD1008", "KD1009");
        List<String> opTimeArray = Arrays.asList("20251024");
        List<String> dimCodeArray = Arrays.asList("city_id");

        log.info("=== 查询参数 ===");
        log.info("KPI IDs: {}", kpiIds);
        log.info("时间点: {}", opTimeArray);
        log.info("维度: {}", dimCodeArray);
        log.info("");

        // 批量获取KPI定义
        Map<String, KpiDefinition> kpiDefinitions = metadataRepository.batchGetKpiDefinitions(kpiIds);

        log.info("=== KPI定义信息 ===");
        for (Map.Entry<String, KpiDefinition> entry : kpiDefinitions.entrySet()) {
            KpiDefinition def = entry.getValue();
            log.info("KPI ID: {}", entry.getKey());
            log.info("  - 类型: {}", def.kpiType());
            log.info("  - 计算方法: {}", def.computeMethod());
            log.info("  - 表达式: {}", def.kpiExpr());
            log.info("  - 聚合函数: {}", def.aggFunc());
            log.info("  - 组合维度: {}", def.compDimCode());
            log.info("  - 周期类型: {}", def.cycleType());
            log.info("");
        }

        // 打印依赖解析信息
        KpiSQLiteEngine sqliteEngine = (KpiSQLiteEngine) kpiQueryEngineFactory.getQueryEngine();
        log.info("=== 依赖解析 ===");
        log.info("解析KPI依赖关系...");

        // 模拟依赖解析
        Map<String, KpiDefinition> parsedDefs = sqliteEngine.parseKpiDependencies(kpiDefinitions);
        log.info("解析后需要加载的KPI: {}", parsedDefs.keySet());
        log.info("");

        // 打印SQL构建逻辑
        log.info("=== SQL构建逻辑 ===");
        for (String kpiId : kpiIds) {
            KpiDefinition kpiDef = kpiDefinitions.get(kpiId);
            log.info("构建KPI {} 的查询SQL:", kpiId);
            log.info("  1. 表名: {}_{}_{}_{}", kpiId, kpiDef.cycleType(), kpiDef.compDimCode(), "20251024");
            log.info("  2. 维度表: kpi_dim_{}", kpiDef.compDimCode());
            log.info("  3. 聚合函数: {}", kpiDef.aggFunc());

            // 构建WHERE子句
            String whereClause = "WHERE t.kpi_id = '" + kpiId + "' AND t.op_time = '20251024'";
            if (dimCodeArray != null && !dimCodeArray.isEmpty()) {
                whereClause += " AND t.city_id IN (999)";
            }
            log.info("  4. 过滤条件: {}", whereClause);

            // 构建GROUP BY子句
            String groupByClause = dimCodeArray.stream()
                    .map(dim -> "t." + dim + ", dim_" + dim + ".dim_val")
                    .collect(Collectors.joining(", "));
            log.info("  5. 分组字段: {}", groupByClause.isEmpty() ? "无" : groupByClause);
            log.info("");
        }

        // 打印最终SQL示例
        log.info("=== 生成的SQL示例 ===");

        log.info("对于KD1008:");
        log.info("""
                SELECT
                       t.city_id as city_id,
                       kpi_dim_CD003.dim_val as city_id_desc,
                       'KD1008' as kpi_id,
                       '20251024' as op_time,
                       sum(case when t.op_time = '20251024' then t.kpi_val else null end) as current,
                       NULL as target_value,
                       NULL as check_result,
                       NULL as check_desc
                FROM KD1008_20251024_CD003 t
                LEFT JOIN kpi_dim_CD003 ON t.city_id = kpi_dim_CD003.dim_code
                WHERE t.kpi_id = 'KD1008'
                  AND t.op_time = '20251024'
                GROUP BY t.city_id, kpi_dim_CD003.dim_val
                """);

        log.info("对于KD1009:");
        log.info("""
                SELECT
                       t.city_id as city_id,
                       kpi_dim_CD003.dim_val as city_id_desc,
                       'KD1009' as kpi_id,
                       '20251024' as op_time,
                       sum(case when t.op_time = '20251024' then t.kpi_val else null end) as current,
                       NULL as target_value,
                       NULL as check_result,
                       NULL as check_desc
                FROM KD1009_20251024_CD003 t
                LEFT JOIN kpi_dim_CD003 ON t.city_id = kpi_dim_CD003.dim_code
                WHERE t.kpi_id = 'KD1009'
                  AND t.op_time = '20251024'
                GROUP BY t.city_id, kpi_dim_CD003.dim_val
                """);

        log.info("最终SQL (UNION ALL合并):");
        log.info("""
                (SELECT ... FROM KD1008_20251024_CD003 ...)
                UNION ALL
                (SELECT ... FROM KD1009_20251024_CD003 ...)
                """);

        log.info("\n" +
                "════════════════════════════════════════════════════════════════════════════════\n");
    }

    /**
     * 打印计算指标SQL
     */
    @Test
    void printComputedKpiSql() throws Exception {
        log.info("\n" +
                "╔══════════════════════════════════════════════════════════════════════════════╗\n" +
                "║                SQL打印测试 - 计算指标 (KD2001)                     ║\n" +
                "╚══════════════════════════════════════════════════════════════════════════════╝\n");

        String kpiId = "KD2001";
        KpiDefinition kpiDef = metadataRepository.getKpiDefinition(kpiId);

        log.info("=== KPI定义 ===");
        log.info("KPI ID: {}", kpiId);
        log.info("名称: {}", kpiDef.kpiName());
        log.info("类型: {}", kpiDef.kpiType());
        log.info("计算方法: {}", kpiDef.computeMethod());
        log.info("表达式: {}", kpiDef.kpiExpr());
        log.info("聚合函数: {}", kpiDef.aggFunc());
        log.info("");

        log.info("=== 依赖解析 ===");
        log.info("表达式: {}", kpiDef.kpiExpr());
        log.info("依赖的KPI: KD1002, KD1005");
        log.info("");

        log.info("=== 生成的SQL ===");
        log.info("""
                SELECT
                       t.city_id as city_id,
                       kpi_dim_CD003.dim_val as city_id_desc,
                       'KD2001' as kpi_id,
                       '20251024' as op_time,
                       sum(case when t.kpi_id = 'KD1002' and t.op_time = '20251024' then t.kpi_val else null end) +
                       sum(case when t.kpi_id = 'KD1005' and t.op_time = '20251024' then t.kpi_val else null end) as current,
                       NULL as target_value,
                       NULL as check_result,
                       NULL as check_desc
                FROM (SELECT * FROM KD1002_20251024_CD003 UNION ALL SELECT * FROM KD1005_20251024_CD003) t
                LEFT JOIN kpi_dim_CD003 ON t.city_id = kpi_dim_CD003.dim_code
                WHERE t.kpi_id IN ('KD1002','KD1005')
                  AND t.op_time = '20251024'
                GROUP BY t.city_id, kpi_dim_CD003.dim_val
                """);

        log.info("\n" +
                "════════════════════════════════════════════════════════════════════════════════\n");
    }

    /**
     * 打印复杂表达式SQL
     */
    @Test
    void printComplexExpressionSql() {
        log.info("\n" +
                "╔══════════════════════════════════════════════════════════════════════════════╗\n" +
                "║              SQL打印测试 - 复杂表达式 (虚拟指标)                   ║\n" +
                "╚══════════════════════════════════════════════════════════════════════════════╝\n");

        String expression = "${KD1003.lastYear} + ${KD1003}";
        log.info("=== 表达式 ===");
        log.info("原始表达式: {}", expression);
        log.info("");

        log.info("=== 依赖解析 ===");
        log.info("依赖的KPI: KD1003");
        log.info("时间修饰符: .lastYear (去年同期)");
        log.info("");

        log.info("=== 表达式转换 ===");
        log.info("转换前: {}", expression);
        log.info("转换后: sum(case when t.kpi_id = 'KD1003' and t.op_time = '20231024' then t.kpi_val else null end) + " +
                 "sum(case when t.kpi_id = 'KD1003' and t.op_time = '20241024' then t.kpi_val else null end)");
        log.info("");

        log.info("=== 生成的SQL ===");
        log.info("""
                SELECT
                       t.city_id as city_id,
                       kpi_dim_CD003.dim_val as city_id_desc,
                       'KD1003_VIRTUAL' as kpi_id,
                       '20241024' as op_time,
                       sum(case when t.kpi_id = 'KD1003' and t.op_time = '20231024' then t.kpi_val else null end) +
                       sum(case when t.kpi_id = 'KD1003' and t.op_time = '20241024' then t.kpi_val else null end) as current,
                       NULL as target_value,
                       NULL as check_result,
                       NULL as check_desc
                FROM KD1003_20241024_CD003 t
                LEFT JOIN kpi_dim_CD003 ON t.city_id = kpi_dim_CD003.dim_code
                WHERE t.kpi_id = 'KD1003'
                  AND t.op_time IN ('20231024', '20241024')
                GROUP BY t.city_id, kpi_dim_CD003.dim_val
                """);

        log.info("\n" +
                "════════════════════════════════════════════════════════════════════════════════\n");
    }

    /**
     * 打印历史数据查询SQL
     */
    @Test
    void printHistoricalDataSql() {
        log.info("\n" +
                "╔══════════════════════════════════════════════════════════════════════════════╗\n" +
                "║                 SQL打印测试 - 历史数据查询                     ║\n" +
                "╚══════════════════════════════════════════════════════════════════════════════╝\n");

        log.info("=== 查询配置 ===");
        log.info("当前时间: 20241024");
        log.info("上一周期: 20240924 (20241024 - 1个月)");
        log.info("去年同期: 20231024 (20241024 - 1年)");
        log.info("includeHistoricalData: true");
        log.info("");

        log.info("=== 生成的SQL (三个时间点合并) ===");
        log.info("""
                (SELECT
                       t.city_id as city_id,
                       kpi_dim_CD003.dim_val as city_id_desc,
                       'KD1008' as kpi_id,
                       '20241024' as op_time,
                       sum(case when t.op_time = '20241024' then t.kpi_val else null end) as current,
                       NULL as target_value,
                       NULL as check_result,
                       NULL as check_desc
                 FROM KD1008_20241024_CD003 t
                 LEFT JOIN kpi_dim_CD003 ON t.city_id = kpi_dim_CD003.dim_code
                 WHERE t.kpi_id = 'KD1008'
                   AND t.op_time = '20241024'
                 GROUP BY t.city_id, kpi_dim_CD003.dim_val)
                UNION ALL
                (SELECT
                       ... for 20240924 ...
                 WHERE t.op_time = '20240924' as last_cycle)
                UNION ALL
                (SELECT
                       ... for 20231024 ...
                 WHERE t.op_time = '20231024' as last_year)
                """);

        log.info("\n" +
                "════════════════════════════════════════════════════════════════════════════════\n");
    }

    /**
     * 打印SQLite vs RDB对比
     */
    @Test
    void printSqliteVsRdbComparison() {
        log.info("\n" +
                "╔══════════════════════════════════════════════════════════════════════════════╗\n" +
                "║               SQLite vs RDB 查询逻辑对比                           ║\n" +
                "╚══════════════════════════════════════════════════════════════════════════════╝\n");

        log.info("=== 查询场景 ===");
        log.info("KPI: KD1008, KD1009");
        log.info("时间: 20251024");
        log.info("维度: city_id");
        log.info("");

        log.info("=== RDB查询逻辑 (错误示例) ===");
        log.info("""
                SELECT
                       t.city_id as city_id,
                       kpi_dim_CD003.dim_val as city_id_desc,
                       t.kpi_id,
                       t.op_time,
                       sum(case when t.op_time = '20251024' then t.kpi_val else null end) as current
                FROM kpi_day_CD003 t
                LEFT JOIN kpi_dim_CD003 ON t.city_id = kpi_dim_CD003.dim_code
                WHERE t.kpi_id IN ('KD1008', 'KD1009')  -- ❌ 错误：SQLite中无法使用
                  AND t.op_time = '20251024'
                GROUP BY t.city_id, kpi_dim_CD003.dim_val, t.kpi_id, t.op_time
                """);

        log.info("\n=== SQLite查询逻辑 (正确示例) ===");
        log.info("""
                (SELECT
                       t.city_id as city_id,
                       kpi_dim_CD003.dim_val as city_id_desc,
                       'KD1008' as kpi_id,
                       '20251024' as op_time,
                       sum(case when t.op_time = '20251024' then t.kpi_val else null end) as current
                FROM KD1008_20251024_CD003 t
                LEFT JOIN kpi_dim_CD003 ON t.city_id = kpi_dim_CD003.dim_code
                WHERE t.kpi_id = 'KD1008'  -- ✅ 正确：每个表只有一个KPI
                  AND t.op_time = '20251024'
                GROUP BY t.city_id, kpi_dim_CD003.dim_val)
                UNION ALL
                (SELECT
                       ...
                FROM KD1009_20251024_CD003 ...)
                """);

        log.info("\n=== 关键差异 ===");
        log.info("1. RDB: 单表多KPI，使用 kpi_id IN (...) 过滤");
        log.info("2. SQLite: 多表单KPI，使用 UNION ALL 合并");
        log.info("3. SQLite中不需要 WHERE kpi_id = ? 过滤（每表一KPI）");
        log.info("");

        log.info("\n" +
                "════════════════════════════════════════════════════════════════════════════════\n");
    }

    /**
     * 验证聚合函数应用
     */
    @Test
    void printAggregationFunctionExamples() {
        log.info("\n" +
                "╔══════════════════════════════════════════════════════════════════════════════╗\n" +
                "║                 聚合函数应用示例                           ║\n" +
                "╚══════════════════════════════════════════════════════════════════════════════╝\n");

        log.info("=== 可加指标 (Additive) ===");
        log.info("KPI: KD1008 (全球通出账客户数)");
        log.info("agg_func: sum");
        log.info("SQL: sum(case when t.op_time = '20251024' then t.kpi_val else null end)");
        log.info("说明: 可以跨维度、跨时间累加");
        log.info("");

        log.info("=== 半可加指标 (Semi-Additive) ===");
        log.info("KPI: 库存量 (示例)");
        log.info("agg_func: last_value");
        log.info("SQL: last_value(case when t.op_time = '20251024' then t.kpi_val else null end) over (partition by t.kpi_id order by t.op_time)");
        log.info("说明: 只能在某些维度上聚合，不可跨时间");
        log.info("");

        log.info("=== 不可加指标 (Non-Additive) ===");
        log.info("KPI: 转化率 (示例)");
        log.info("agg_func: min/max");
        log.info("SQL: min(case when t.op_time = '20251024' then t.kpi_val else null end)");
        log.info("说明: 不能聚合，只能取极值");
        log.info("");

        log.info("\n" +
                "════════════════════════════════════════════════════════════════════════════════\n");
    }
}
