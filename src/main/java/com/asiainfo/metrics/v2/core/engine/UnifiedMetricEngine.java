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
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.MeterRegistry;
import io.quarkus.redis.datasource.RedisDataSource;
import io.quarkus.redis.datasource.value.ValueCommands;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.micrometer.core.instrument.Timer;

import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

/**
 * 统一指标引擎 (Production Ready)
 * - 集成 Redis 结果级缓存
 * - 异步 IO 并发加载
 * - 自动维度表关联
 */
@ApplicationScoped
public class UnifiedMetricEngine {

    private static final Logger log = LoggerFactory.getLogger(UnifiedMetricEngine.class);
    private static final int ATTACH_THRESHOLD = 8;
    private static final String CACHE_PREFIX = "metrics:v2:query:";

    @Inject MetricParser parser;
    @Inject SqlGenerator sqlGenerator;
    @Inject SQLiteExecutor sqliteExecutor;
    @Inject MetadataRepository metadataRepo;
    @Inject StorageManager storageManager;

    @Inject RedisDataSource redisDataSource;
    @Inject ObjectMapper objectMapper;
    @Inject MeterRegistry registry;

    @ConfigProperty(name = "kpi.cache.ttl.minutes", defaultValue = "30")
    long cacheTtlMinutes;

    // 使用 JDK 21 正式特性的虚拟线程池
    private final ExecutorService vThreadExecutor = Executors.newVirtualThreadPerTaskExecutor();

    public List<Map<String, Object>> execute(KpiQueryRequest req) {
        // 2. 启动计时器
        Timer.Sample sample = Timer.start(registry);
        String cacheStatus = "miss";

        try {
            String cacheKey = generateCacheKey(req);
            ValueCommands<String, String> redisCommands = null;

            // --- 缓存读取 ---
            try {
                redisCommands = redisDataSource.value(String.class);
                String cachedValue = redisCommands.get(cacheKey);
                if (cachedValue != null) {
                    log.info("Cache HIT: {}", cacheKey);
                    cacheStatus = "hit"; // 标记命中
                    return objectMapper.readValue(cachedValue, new TypeReference<List<Map<String, Object>>>() {});
                }
            } catch (Exception e) {
                log.warn("Redis read failed: {}", e.getMessage());
                cacheStatus = "error";
            }

            log.info("Cache MISS, executing query...");
            List<Map<String, Object>> finalResults = new ArrayList<>();

            // --- 核心计算 ---
            List<MetricDefinition> taskMetrics = expandMetrics(req.kpiArray(), req.includeHistoricalData());
            for (String opTime : req.opTimeArray()) {
                try {
                    List<Map<String, Object>> batchResults = executeSingleTimePoint(req, taskMetrics, opTime);
                    finalResults.addAll(batchResults);
                } catch (Exception e) {
                    log.error("Query failed for opTime: {}", opTime, e);
                    throw new RuntimeException("Query failed for opTime: " + opTime, e);
                }
            }

            // --- 缓存写入 ---
            try {
                if (!finalResults.isEmpty() && redisCommands != null) {
                    String jsonResult = objectMapper.writeValueAsString(finalResults);
                    redisCommands.setex(cacheKey, cacheTtlMinutes * 60, jsonResult);
                }
            } catch (Exception e) {
                log.warn("Redis write failed: {}", e.getMessage());
            }

            return finalResults;

        } finally {
            // 3. 停止计时并记录指标
            sample.stop(Timer.builder("metrics.req.duration")
                    .description("API request duration")
                    .tag("cache", cacheStatus) // 关键标签：区分缓存命中与否
                    .register(registry));
        }
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

        // A. 下载 KPI 数据表
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
        List<MetricDefinition> tasks = new ArrayList<>();
        boolean loadHistory = Boolean.TRUE.equals(includeHistorical);

        for (String kpiInput : kpiArray) {
            MetricDefinition baseDef;
            if (kpiInput.startsWith("${")) {
                String id = "V_" + Math.abs(kpiInput.hashCode());
                baseDef = MetricDefinition.virtual(id, kpiInput, "sum");
            } else {
                MetricDefinition metaDef = metadataRepo.findById(kpiInput);
                // 容错：如果没有定义，使用默认物理指标
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

    /**
     * 生成确定性的缓存 Key
     */
    private String generateCacheKey(KpiQueryRequest req) {
        StringBuilder sb = new StringBuilder(CACHE_PREFIX);

        // KPI 列表 (排序后拼接)
        List<String> sortedKpis = new ArrayList<>(req.kpiArray());
        Collections.sort(sortedKpis);
        sb.append("kpis:").append(String.join(",", sortedKpis)).append("|");

        // 时间列表
        List<String> sortedTimes = new ArrayList<>(req.opTimeArray());
        Collections.sort(sortedTimes);
        sb.append("times:").append(String.join(",", sortedTimes)).append("|");

        // 维度列表
        if (req.dimCodeArray() != null && !req.dimCodeArray().isEmpty()) {
            List<String> sortedDims = new ArrayList<>(req.dimCodeArray());
            Collections.sort(sortedDims);
            sb.append("dims:").append(String.join(",", sortedDims)).append("|");
        }

        // 过滤条件 (简单处理，假设顺序一致)
        if (req.dimConditionArray() != null && !req.dimConditionArray().isEmpty()) {
            sb.append("conds:").append(req.dimConditionArray().hashCode()).append("|");
        }

        // 标志位
        sb.append("hist:").append(req.includeHistoricalData()).append("|");
        sb.append("target:").append(req.includeTargetData());

        return sb.toString();
    }
}