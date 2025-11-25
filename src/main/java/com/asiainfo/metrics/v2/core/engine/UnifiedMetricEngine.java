package com.asiainfo.metrics.v2.core.engine;

import com.asiainfo.metrics.model.http.KpiQueryRequest;
import com.asiainfo.metrics.v2.core.generator.SqlGenerator;
import com.asiainfo.metrics.v2.core.model.MetricDefinition;
import com.asiainfo.metrics.v2.core.model.MetricType;
import com.asiainfo.metrics.v2.core.model.PhysicalTableReq;
import com.asiainfo.metrics.v2.core.model.QueryContext;
import com.asiainfo.metrics.v2.core.parser.MetricParser;
import com.asiainfo.metrics.v2.infra.persistence.MetadataRepository;
import com.asiainfo.metrics.v2.infra.persistence.SQLiteExecutor;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * 统一指标引擎 (Production Ready)
 */
@ApplicationScoped
public class UnifiedMetricEngine {

    private static final Logger log = LoggerFactory.getLogger(UnifiedMetricEngine.class);
    private static final String DEFAULT_COMP_DIM_CODE = "CD003";

    // 阈值：超过此数量的文件将触发分批加载策略
    private static final int ATTACH_THRESHOLD = 8;

    @Inject MetricParser parser;
    @Inject SqlGenerator sqlGenerator;
    @Inject SQLiteExecutor sqliteExecutor;
    @Inject MetadataRepository metadataRepo;

    public List<Map<String, Object>> execute(KpiQueryRequest req) {
        log.info("Starting query: KPIs={}, Times={}", req.kpiArray(), req.opTimeArray());
        List<Map<String, Object>> finalResults = new ArrayList<>();

        // 1. 任务裂变
        List<MetricDefinition> taskMetrics = expandMetrics(req.kpiArray(), req.includeHistoricalData());

        // 2. 按时间点执行
        for (String opTime : req.opTimeArray()) {
            try {
                List<Map<String, Object>> batchResults = executeSingleTimePoint(req, taskMetrics, opTime);
                finalResults.addAll(batchResults);
            } catch (Exception e) {
                log.error("Query failed for opTime: {}", opTime, e);
                throw new RuntimeException("Query failed for opTime: " + opTime, e);
            }
        }
        return finalResults;
    }

    private List<Map<String, Object>> executeSingleTimePoint(
            KpiQueryRequest req,
            List<MetricDefinition> taskMetrics,
            String opTime) {

        QueryContext ctx = new QueryContext();
        ctx.setOpTime(opTime);
        ctx.setIncludeHistorical(req.includeHistoricalData());
        ctx.setIncludeTarget(req.includeTargetData());

        if(req.dimCodeArray() != null) {
            req.dimCodeArray().forEach(ctx::addDimCode);
        }

        String compDimCode = DEFAULT_COMP_DIM_CODE; // 实际应从请求获取
        ctx.setCompDimCode(compDimCode);

        // 1. 依赖解析
        for (MetricDefinition metric : taskMetrics) {
            parser.resolveDependencies(metric, opTime, compDimCode, ctx);
        }

        // 2. 策略选择与执行
        int tableCount = ctx.getRequiredTables().size();
        List<String> dims = req.dimCodeArray() != null ? req.dimCodeArray() : new ArrayList<>();

        List<Map<String, Object>> results;

        if (tableCount > ATTACH_THRESHOLD) {
            log.info("Physical tables count ({}) exceeds threshold, using Staging Strategy.", tableCount);
            // 策略 B: 分批加载 (Staging)
            results = sqliteExecutor.executeWithStaging(ctx, dims,
                    (tableName) -> sqlGenerator.generateSqlWithStaging(taskMetrics, ctx, dims, tableName)
            );
        } else {
            log.debug("Physical tables count ({}), using Standard Strategy.", tableCount);
            // 策略 A: 标准 Attach (Standard)
            // 此时仍需要给每个表分配别名，以便生成 SQL
            prepareAliases(ctx);
            String sql = sqlGenerator.generateSql(taskMetrics, ctx, dims);
            results = sqliteExecutor.executeQuery(ctx, sql);
        }

        // 3. 回填时间
        results.forEach(row -> row.put("op_time", opTime));
        return results;
    }

    // 辅助：为标准模式分配别名
    private void prepareAliases(QueryContext ctx) {
        for (PhysicalTableReq req : ctx.getRequiredTables()) {
            String alias = "db_" + Math.abs((req.kpiId() + req.opTime()).hashCode());
            ctx.registerAlias(req, alias);
        }
    }

    private List<MetricDefinition> expandMetrics(List<String> kpiArray, Boolean includeHistorical) {
        // ... (保持原有的 expandMetrics 逻辑不变)
        // 为节省篇幅，这里引用原有代码逻辑
        List<MetricDefinition> tasks = new ArrayList<>();
        boolean loadHistory = Boolean.TRUE.equals(includeHistorical);

        for (String kpiInput : kpiArray) {
            MetricDefinition baseDef;
            if (kpiInput.startsWith("${")) {
                String id = "V_" + Math.abs(kpiInput.hashCode());
                baseDef = MetricDefinition.virtual(id, kpiInput, "sum");
            } else {
                MetricDefinition metaDef = metadataRepo.findById(kpiInput);
                baseDef = (metaDef != null) ? metaDef : MetricDefinition.physical(kpiInput, "sum");
            }
            tasks.add(baseDef);

            if (loadHistory) {
                if (baseDef.type() != MetricType.VIRTUAL) {
                    tasks.add(new MetricDefinition(baseDef.id() + "_lastYear", "${" + baseDef.id() + ".lastYear}", MetricType.COMPOSITE, baseDef.aggFunc()));
                    tasks.add(new MetricDefinition(baseDef.id() + "_lastCycle", "${" + baseDef.id() + ".lastCycle}", MetricType.COMPOSITE, baseDef.aggFunc()));
                }
            }
        }
        return tasks;
    }
}