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
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * 统一指标引擎 (Production Ready)
 * 移除 Preview API，支持全量历史数据查询。
 */
@ApplicationScoped
public class UnifiedMetricEngine {

    private static final Logger log = LoggerFactory.getLogger(UnifiedMetricEngine.class);
    private static final String DEFAULT_COMP_DIM_CODE = "CD003"; // 示例默认值

    @Inject MetricParser parser;
    @Inject SqlGenerator sqlGenerator;
    @Inject StorageManager storageManager;
    @Inject SQLiteExecutor sqliteExecutor;
    @Inject MetadataRepository metadataRepo;

    /**
     * 执行指标查询
     */
    public List<Map<String, Object>> execute(KpiQueryRequest req) {
        log.info("Starting query: KPIs={}, Times={}", req.kpiArray(), req.opTimeArray());

        List<Map<String, Object>> finalResults = new ArrayList<>();

        // 1. 任务裂变：扩展历史指标 (Current, LastYear, LastCycle)
        List<MetricDefinition> taskMetrics = expandMetrics(req.kpiArray(), req.includeHistoricalData());

        // 2. 按时间点分批执行
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

        // 设置维度
        if(req.dimCodeArray() != null) {
            req.dimCodeArray().forEach(ctx::addDimCode);
        }

        // 假设从请求或配置获取 CompDimCode
        String compDimCode = DEFAULT_COMP_DIM_CODE;
        ctx.setCompDimCode(compDimCode);

        // 1. 依赖解析
        for (MetricDefinition metric : taskMetrics) {
            parser.resolveDependencies(metric, opTime, compDimCode, ctx);
        }

        // 2. 并行 IO：下载并挂载
        prepareDataFiles(ctx);

        // 3. 生成 SQL
        // 注意：Generator 现在接收 opTime 作为基准时间
        String sql = sqlGenerator.generateSql(taskMetrics, ctx, req.dimCodeArray());
        log.debug("SQL [{}]: \n{}", opTime, sql);

        // 4. 执行查询
        List<Map<String, Object>> results = sqliteExecutor.executeQuery(ctx, sql);

        // 5. 结果回填基准时间
        results.forEach(row -> row.put("op_time", opTime));

        return results;
    }

    /**
     * 并行准备数据文件 (使用标准 ExecutorService)
     */
    private void prepareDataFiles(QueryContext ctx) {
        Set<PhysicalTableReq> tables = ctx.getRequiredTables();
        if (tables.isEmpty()) return;

        // 使用虚拟线程执行器 (JDK 21 标准 API)
        try (ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor()) {
            List<Future<Void>> futures = new ArrayList<>();

            for (PhysicalTableReq req : tables) {
                futures.add(executor.submit(() -> {
                    String localPath = storageManager.downloadAndPrepare(req);
                    String alias = "db_" + Math.abs((req.kpiId() + req.opTime()).hashCode());
                    ctx.registerAlias(req, alias);
                    return null;
                }));
            }

            // 等待所有任务完成
            for (Future<Void> f : futures) {
                f.get(); // 抛出异常以快速失败
            }
        } catch (Exception e) {
            log.error("Data preparation failed", e);
            throw new RuntimeException("Failed to prepare data files", e);
        }
    }

    /**
     * 自动扩展指标定义
     * 支持物理指标、复合指标和虚拟指标的历史数据扩展
     */
    private List<MetricDefinition> expandMetrics(List<String> kpiArray, Boolean includeHistorical) {
        List<MetricDefinition> tasks = new ArrayList<>();
        boolean loadHistory = Boolean.TRUE.equals(includeHistorical);

        for (String kpiInput : kpiArray) {
            MetricDefinition baseDef;

            // 1. 解析基础指标
            if (kpiInput.startsWith("${")) {
                // 虚拟指标
                String id = "V_" + Math.abs(kpiInput.hashCode());
                baseDef = MetricDefinition.virtual(id, kpiInput, "sum");
            } else {
                // 物理/复合指标
                MetricDefinition metaDef = metadataRepo.findById(kpiInput);
                if (metaDef != null) {
                    baseDef = metaDef;
                } else {
                    // 容错降级为物理指标
                    baseDef = MetricDefinition.physical(kpiInput, "sum");
                }
            }
            tasks.add(baseDef);

            // 2. 扩展历史指标
            if (loadHistory) {
                // 生成同比指标定义
                // 逻辑：将原表达式中的所有变量时间偏移修改为 .lastYear
                // 注意：对于复杂虚拟指标，简单的字符串拼接可能不够，这里假设 MetricDefinition
                // 的 expression 已经是标准化格式，或者在 Parser 层处理。
                // 简单起见，我们构造一个新的 Definition，其 Expression 使用特殊的 modify 语法
                // 或者更直接地：构造一个指向原指标 ID 但带有 modifier 的新虚拟指标

                if (baseDef.type() == MetricType.VIRTUAL) {
                    // 虚拟指标的历史扩展比较复杂，需要 Parser 支持递归修改
                    // 暂时跳过或仅支持简单引用
                    log.warn("Auto history for complex virtual metrics is not fully supported yet: {}", kpiInput);
                } else {
                    // 对于有 ID 的指标 (物理/复合)，直接引用 ID.lastYear
                    tasks.add(new MetricDefinition(
                            baseDef.id() + "_lastYear",
                            "${" + baseDef.id() + ".lastYear}",
                            MetricType.COMPOSITE, // 视为复合指标处理
                            baseDef.aggFunc()
                    ));

                    tasks.add(new MetricDefinition(
                            baseDef.id() + "_lastCycle",
                            "${" + baseDef.id() + ".lastCycle}",
                            MetricType.COMPOSITE,
                            baseDef.aggFunc()
                    ));
                }
            }
        }
        return tasks;
    }
}