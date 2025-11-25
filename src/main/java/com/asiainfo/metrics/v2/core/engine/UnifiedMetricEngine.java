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

import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

@ApplicationScoped
public class UnifiedMetricEngine {

    private static final Logger log = LoggerFactory.getLogger(UnifiedMetricEngine.class);
    private static final int ATTACH_THRESHOLD = 8;

    @Inject MetricParser parser;
    @Inject SqlGenerator sqlGenerator;
    @Inject SQLiteExecutor sqliteExecutor;
    @Inject MetadataRepository metadataRepo;
    @Inject StorageManager storageManager;

    private final ExecutorService vThreadExecutor = Executors.newVirtualThreadPerTaskExecutor();

    public List<Map<String, Object>> execute(KpiQueryRequest req) {
        List<Map<String, Object>> finalResults = new ArrayList<>();
        List<MetricDefinition> taskMetrics = expandMetrics(req.kpiArray(), req.includeHistoricalData());

        for (String opTime : req.opTimeArray()) {
            try {
                finalResults.addAll(executeSingleTimePoint(req, taskMetrics, opTime));
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

        // 1. 解析阶段
        for (MetricDefinition metric : taskMetrics) {
            parser.resolveDependencies(metric, opTime, ctx);
        }

        // 2. IO 准备 (并行下载 KPI表 和 维度表)
        preparePhysicalTables(ctx);

        // 3. SQL 生成与执行
        int tableCount = ctx.getRequiredTables().size();
        List<String> dims = req.dimCodeArray() != null ? req.dimCodeArray() : new ArrayList<>();

        List<Map<String, Object>> results;
        if (tableCount > ATTACH_THRESHOLD) {
            results = sqliteExecutor.executeWithStaging(ctx, dims,
                    (tableName) -> sqlGenerator.generateSqlWithStaging(taskMetrics, ctx, dims, tableName)
            );
        } else {
            String sql = sqlGenerator.generateSql(taskMetrics, ctx, dims);
            results = sqliteExecutor.executeQuery(ctx, sql);
        }

        results.forEach(row -> row.put("op_time", opTime));
        return results;
    }

    private void preparePhysicalTables(QueryContext ctx) {
        List<Callable<Void>> tasks = new ArrayList<>();

        // A. 任务：下载 KPI 数据表
        for (PhysicalTableReq req : ctx.getRequiredTables()) {
            tasks.add(() -> {
                storageManager.downloadAndPrepare(req);
                String alias = "db_" + Math.abs((req.kpiId() + req.opTime()).hashCode());
                ctx.registerAlias(req, alias);
                return null;
            });
        }

        // B. 任务：下载 维度表 (新增逻辑)
        // 提取所有涉及的 compDimCode
        Set<String> distinctCompDimCodes = ctx.getRequiredTables().stream()
                .map(PhysicalTableReq::compDimCode)
                .filter(Objects::nonNull)
                .collect(Collectors.toSet());

        for (String compDimCode : distinctCompDimCodes) {
            tasks.add(() -> {
                // 调用 StorageManager 下载维度表
                String localPath = storageManager.downloadAndCacheDimDB(compDimCode);
                // 注册到 Context，供 Executor 使用
                ctx.addDimensionTablePath(compDimCode, localPath);
                return null;
            });
        }

        try {
            List<Future<Void>> futures = vThreadExecutor.invokeAll(tasks);
            for (Future<Void> f : futures) f.get();
        } catch (Exception e) {
            throw new RuntimeException("Failed to prepare tables", e);
        }
    }

    private List<MetricDefinition> expandMetrics(List<String> kpiArray, Boolean includeHistorical) {
        // ... 保持不变 ...
        List<MetricDefinition> tasks = new ArrayList<>();
        boolean loadHistory = Boolean.TRUE.equals(includeHistorical);

        for (String kpiInput : kpiArray) {
            MetricDefinition baseDef;
            if (kpiInput.startsWith("${")) {
                String id = "V_" + Math.abs(kpiInput.hashCode());
                baseDef = MetricDefinition.virtual(id, kpiInput, "sum");
            } else {
                MetricDefinition metaDef = metadataRepo.findById(kpiInput);
                baseDef = (metaDef != null) ? metaDef : MetricDefinition.physical(kpiInput, "sum", "CD003");
            }
            tasks.add(baseDef);

            if (loadHistory && baseDef.type() != MetricType.VIRTUAL) {
                tasks.add(new MetricDefinition(baseDef.id() + "_lastYear", "${" + baseDef.id() + ".lastYear}", MetricType.COMPOSITE, baseDef.aggFunc(), baseDef.compDimCode()));
                tasks.add(new MetricDefinition(baseDef.id() + "_lastCycle", "${" + baseDef.id() + ".lastCycle}", MetricType.COMPOSITE, baseDef.aggFunc(), baseDef.compDimCode()));
            }
        }
        return tasks;
    }
}