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

@ApplicationScoped
public class UnifiedMetricEngine {

    private static final Logger log = LoggerFactory.getLogger(UnifiedMetricEngine.class);
    private static final int ATTACH_THRESHOLD = 8;
    // 移除: private static final String DEFAULT_COMP_DIM_CODE = "CD003";

    @Inject MetricParser parser;
    @Inject SqlGenerator sqlGenerator;
    @Inject SQLiteExecutor sqliteExecutor;
    @Inject MetadataRepository metadataRepo;
    @Inject StorageManager storageManager;

    private final ExecutorService vThreadExecutor = Executors.newVirtualThreadPerTaskExecutor();

    public List<Map<String, Object>> execute(KpiQueryRequest req) {
        List<Map<String, Object>> finalResults = new ArrayList<>();
        // 1. 预处理：此处会从元数据加载 compDimCode
        List<MetricDefinition> taskMetrics = expandMetrics(req.kpiArray(), req.includeHistoricalData());

        for (String opTime : req.opTimeArray()) {
            try {
                finalResults.addAll(executeSingleTimePoint(req, taskMetrics, opTime));
            } catch (Exception e) {
                log.error("Query failed for opTime: {}", opTime, e);
                throw new RuntimeException(e);
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

        if(req.dimCodeArray() != null) req.dimCodeArray().forEach(ctx::addDimCode);

        // 移除: ctx.setCompDimCode(DEFAULT_COMP_DIM_CODE);
        // 现在 compDimCode 包含在 taskMetrics 的元素中，或者由 parser 动态解析

        // 1. 依赖解析
        for (MetricDefinition metric : taskMetrics) {
            // 修改：不再传递 compDimCode，Parser 会自己处理
            parser.resolveDependencies(metric, opTime, ctx);
        }

        // 2. IO 准备
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
            // SqlGenerator 现在会遍历 ctx 中的表来确定使用哪些维度表
            String sql = sqlGenerator.generateSql(taskMetrics, ctx, dims);
            results = sqliteExecutor.executeQuery(ctx, sql);
        }

        results.forEach(row -> row.put("op_time", opTime));
        return results;
    }

    private void preparePhysicalTables(QueryContext ctx) {
        // ... 代码与之前一致，省略 ...
        // 关键点：ctx.getRequiredTables() 里的 PhysicalTableReq
        // 已经在 parser 阶段被正确填充了各自的 compDimCode
        List<Callable<Void>> tasks = new ArrayList<>();
        for (PhysicalTableReq req : ctx.getRequiredTables()) {
            tasks.add(() -> {
                storageManager.downloadAndPrepare(req);
                String alias = "db_" + Math.abs((req.kpiId() + req.opTime()).hashCode());
                ctx.registerAlias(req, alias);
                return null;
            });
        }
        // ... invokeAll logic ...
        try {
            List<Future<Void>> futures = vThreadExecutor.invokeAll(tasks);
            for (Future<Void> f : futures) f.get();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private List<MetricDefinition> expandMetrics(List<String> kpiArray, Boolean includeHistorical) {
        List<MetricDefinition> tasks = new ArrayList<>();
        boolean loadHistory = Boolean.TRUE.equals(includeHistorical);

        for (String kpiInput : kpiArray) {
            MetricDefinition baseDef;
            if (kpiInput.startsWith("${")) {
                // 虚拟指标，compDimCode 为 null，解析时由依赖决定
                String id = "V_" + Math.abs(kpiInput.hashCode());
                baseDef = MetricDefinition.virtual(id, kpiInput, "sum");
            } else {
                // 物理/复合指标，从 Repo 加载，此时 compDimCode 会被填充
                MetricDefinition metaDef = metadataRepo.findById(kpiInput);
                // 如果 metaDef 为空（不应该），回退到默认，但这里最好抛错
                baseDef = (metaDef != null) ? metaDef : MetricDefinition.physical(kpiInput, "sum", "CD003");
            }
            tasks.add(baseDef);

            // 扩展历史指标 (历史指标复用 baseDef 的 compDimCode)
            if (loadHistory && baseDef.type() != MetricType.VIRTUAL) {
                // 构造历史指标定义，继承原指标的 compDimCode
                tasks.add(new MetricDefinition(
                        baseDef.id() + "_lastYear",
                        "${" + baseDef.id() + ".lastYear}",
                        MetricType.COMPOSITE,
                        baseDef.aggFunc(),
                        baseDef.compDimCode() // 继承
                ));
                tasks.add(new MetricDefinition(
                        baseDef.id() + "_lastCycle",
                        "${" + baseDef.id() + ".lastCycle}",
                        MetricType.COMPOSITE,
                        baseDef.aggFunc(),
                        baseDef.compDimCode() // 继承
                ));
            }
        }
        return tasks;
    }
}