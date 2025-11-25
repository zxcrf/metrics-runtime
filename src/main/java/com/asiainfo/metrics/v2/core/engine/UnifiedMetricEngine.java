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
import com.asiainfo.metrics.v2.infra.storage.StorageManager;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

/**
 * 统一指标引擎 (Production Ready)
 */
@ApplicationScoped
public class UnifiedMetricEngine {
    private static final Logger log = LoggerFactory.getLogger(UnifiedMetricEngine.class);
    private static final int ATTACH_THRESHOLD = 8;

    @Inject MetricParser parser;
    @Inject SqlGenerator sqlGenerator;
    @Inject SQLiteExecutor sqliteExecutor;
    @Inject StorageManager storageManager;
    @Inject MetadataRepository metadataRepo;

    // 使用虚拟线程池，这是 JDK 21 的正式特性
    private final ExecutorService vThreadExecutor = Executors.newVirtualThreadPerTaskExecutor();

    public List<Map<String, Object>> execute(KpiQueryRequest req) {
        List<Map<String, Object>> finalResults = new ArrayList<>();
        // 1. 预处理：将请求中的 KPI 转换为内部定义对象（处理虚拟指标等）
        List<MetricDefinition> taskMetrics = expandMetrics(req.kpiArray(), req.includeHistoricalData());

        // 2. 按时间点裂变任务 (Time-Slicing Pattern)
        // 这种模式下，Context 中的 opTime 是 String 是合理的
        for (String opTime : req.opTimeArray()) {
            finalResults.addAll(executeSingleTimePoint(req, taskMetrics, opTime));
        }
        return finalResults;
    }

    private List<Map<String, Object>> executeSingleTimePoint(
            KpiQueryRequest req,
            List<MetricDefinition> taskMetrics,
            String opTime) {

        QueryContext ctx = new QueryContext();
        ctx.setOpTime(opTime); // 上下文绑定当前切片时间
        ctx.setIncludeHistorical(req.includeHistoricalData());
        ctx.setIncludeTarget(req.includeTargetData());

        // 注入请求的维度信息
        if (req.dimCodeArray() != null) {
            req.dimCodeArray().forEach(ctx::addDimCode);
        }

        // 【关键重构】不再在 Context 中强行绑定单一 compDimCode
        // 实际的维度表名 (kpi_dim_xxx) 应该由 Generator 根据具体的维度需求生成
        // 这里如果必须兼容旧逻辑，可以设为 null 或主要维度
        // ctx.setCompDimCode(...)

        // 1. 依赖解析
        // 解析器会分析指标依赖哪些物理表，并注册到 ctx.requiredTables
        for (MetricDefinition metric : taskMetrics) {
            // 这里传入 null 作为 compDimCode，由 KPI 定义自带的属性决定
            parser.resolveDependencies(metric, opTime, null, ctx);
        }
        int tableCount = ctx.getRequiredTables().size();

        // 2. 并行 IO 准备 (替代 StructuredTaskScope)
        preparePhysicalTables(ctx);

        // 3. SQL 生成与执行 (计算密集型)
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

    /**
     * 使用标准 ExecutorService 实现并发下载
     */
    private void preparePhysicalTables(QueryContext ctx) {
        List<Callable<Void>> tasks = new ArrayList<>();

        for (PhysicalTableReq req : ctx.getRequiredTables()) {
            tasks.add(() -> {
                // 1. 下载/检查文件
                storageManager.downloadAndPrepare(req);

                // 2. 注册别名 (db_hash)
                String alias = "db_" + Math.abs((req.kpiId() + req.opTime()).hashCode());
                ctx.registerAlias(req, alias);
                return null;
            });
        }

        try {
            // invokeAll 会等待所有任务完成，且利用虚拟线程并行执行
            List<Future<Void>> futures = vThreadExecutor.invokeAll(tasks);

            // 检查异常
            for (Future<Void> f : futures) {
                f.get(); // 如果任务抛出异常，这里会重新抛出
            }
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException("并行加载物理表失败", e);
        }
    }
}