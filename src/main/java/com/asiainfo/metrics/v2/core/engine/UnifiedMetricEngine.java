package com.asiainfo.metrics.v2.core.engine;

import com.asiainfo.metrics.model.http.KpiQueryRequest;
import com.asiainfo.metrics.v2.core.generator.SqlGenerator;
import com.asiainfo.metrics.v2.core.model.*;
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
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * 统一指标引擎 (Production Ready)
 * 重构点：
 * 1. 移除 StructuredTaskScope (Preview API)，使用标准 ExecutorService
 * 2. 显式分离 IO 准备阶段与 SQL 执行阶段
 * 3. 修复 QueryContext 并发问题
 */
@ApplicationScoped
public class UnifiedMetricEngine {

    private static final Logger log = LoggerFactory.getLogger(UnifiedMetricEngine.class);
    private static final int ATTACH_THRESHOLD = 8;
    private static final String DEFAULT_COMP_DIM_CODE = "CD003"; // 兜底默认值

    @Inject MetricParser parser;
    @Inject SqlGenerator sqlGenerator;
    @Inject SQLiteExecutor sqliteExecutor;
    @Inject MetadataRepository metadataRepo;
    @Inject StorageManager storageManager;

    // 使用 JDK 21 正式特性的虚拟线程池
    private final ExecutorService vThreadExecutor = Executors.newVirtualThreadPerTaskExecutor();

    public List<Map<String, Object>> execute(KpiQueryRequest req) {
        log.info("Starting query V2: KPIs={}, Times={}", req.kpiArray(), req.opTimeArray());
        List<Map<String, Object>> finalResults = new ArrayList<>();

        // 1. 任务预处理与裂变
        List<MetricDefinition> taskMetrics = expandMetrics(req.kpiArray(), req.includeHistoricalData());

        // 2. 按时间点切片执行 (Time-Slicing)
        // 这种设计规避了在 SQL 层处理复杂的时间数组逻辑，不仅简化了 Generator，也更容易并行化
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

        // 注入维度信息
        if(req.dimCodeArray() != null) {
            req.dimCodeArray().forEach(ctx::addDimCode);
        }

        // 确定主维度 (用于某些默认行为，但不再强依赖)
        // 实际业务中可能需要根据 KPI 定义动态推断，这里暂取默认
        ctx.setCompDimCode(DEFAULT_COMP_DIM_CODE);

        // 1. 【解析阶段】计算依赖图 (CPU 密集型 - 轻量)
        for (MetricDefinition metric : taskMetrics) {
            // 解析器会将依赖的物理表注册到 ctx.requiredTables
            parser.resolveDependencies(metric, opTime, ctx.getCompDimCode(), ctx);
        }

        // 2. 【IO 阶段】并行下载所有物理表文件 (IO 密集型)
        // 这一步完成后，本地文件系统一定存在所需的 .db 文件
        preparePhysicalTables(ctx);

        // 3. 【执行阶段】纯内存计算/SQL执行 (CPU/内存 密集型)
        // 此时 SQLiteExecutor 不需要再进行网络 IO
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
            String sql = sqlGenerator.generateSql(taskMetrics, ctx, dims);
            results = sqliteExecutor.executeQuery(ctx, sql);
        }

        // 回填时间字段
        results.forEach(row -> row.put("op_time", opTime));
        return results;
    }

    /**
     * 并行下载物理表文件
     * 使用 ExecutorService + VirtualThread 替代 StructuredTaskScope
     */
    private void preparePhysicalTables(QueryContext ctx) {
        List<Callable<Void>> tasks = new ArrayList<>();

        for (PhysicalTableReq req : ctx.getRequiredTables()) {
            tasks.add(() -> {
                // 1. 下载/检查文件 (幂等操作)
                storageManager.downloadAndPrepare(req);

                // 2. 注册别名 (用于 SQL 生成)
                // 别名必须是合法的 SQL 标识符，使用 Hash 保证唯一性
                String alias = "db_" + Math.abs((req.kpiId() + req.opTime()).hashCode());
                ctx.registerAlias(req, alias);
                return null;
            });
        }

        try {
            // invokeAll 会阻塞直到所有任务完成
            // 如果其中有任务抛出异常，将在 get() 时被捕获
            List<Future<Void>> futures = vThreadExecutor.invokeAll(tasks);

            for (Future<Void> f : futures) {
                f.get(); // 检查异常，如果有下载失败，这里会抛出 ExecutionException
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to download physical tables concurrently", e);
        }
    }

    private List<MetricDefinition> expandMetrics(List<String> kpiArray, Boolean includeHistorical) {
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

            // 自动扩展历史指标
            if (loadHistory && baseDef.type() != MetricType.VIRTUAL) {
                tasks.add(new MetricDefinition(baseDef.id() + "_lastYear", "${" + baseDef.id() + ".lastYear}", MetricType.COMPOSITE, baseDef.aggFunc()));
                tasks.add(new MetricDefinition(baseDef.id() + "_lastCycle", "${" + baseDef.id() + ".lastCycle}", MetricType.COMPOSITE, baseDef.aggFunc()));
            }
        }
        return tasks;
    }
}