package com.asiainfo.metrics.v2.core.engine;

import com.asiainfo.metrics.model.http.KpiQueryRequest;
import com.asiainfo.metrics.v2.core.generator.SqlGenerator;
import com.asiainfo.metrics.v2.core.model.*;
import com.asiainfo.metrics.v2.core.parser.MetricParser;
import com.asiainfo.metrics.v2.infra.persistence.MetadataRepository;
import com.asiainfo.metrics.v2.infra.persistence.SQLiteExecutor;
import com.asiainfo.metrics.v2.infra.storage.StorageManager;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.quarkus.redis.datasource.RedisDataSource;
import io.quarkus.redis.datasource.value.ValueCommands;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.eclipse.microprofile.config.inject.ConfigProperty;
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
    private static final String CACHE_PREFIX = "metrics:v2:query:";

    @Inject
    MetricParser parser;
    @Inject
    SqlGenerator sqlGenerator;
    @Inject
    SQLiteExecutor sqliteExecutor;
    @Inject
    MetadataRepository metadataRepo;
    @Inject
    StorageManager storageManager;
    @Inject
    RedisDataSource redisDataSource;
    @Inject
    ObjectMapper objectMapper;
    @Inject
    MeterRegistry registry;

    @ConfigProperty(name = "kpi.cache.ttl.minutes", defaultValue = "30")
    long cacheTtlMinutes;

    private final ExecutorService vThreadExecutor = Executors.newVirtualThreadPerTaskExecutor();

    public List<Map<String, Object>> execute(KpiQueryRequest req) {
        // ========== 总计时开始 ==========
        long t0 = System.currentTimeMillis();
        Timer.Sample sample = Timer.start(registry);
        String cacheStatus = "miss";

        try {
            // ========== 阶段 1: 缓存检查 ==========
            long t1 = System.currentTimeMillis();
            String cacheKey = generateCacheKey(req);
            ValueCommands<String, String> redisCommands = null;

            try {
                redisCommands = redisDataSource.value(String.class);
                String cachedValue = redisCommands.get(cacheKey);
                if (cachedValue != null) {
                    long cacheTime = System.currentTimeMillis() - t1;
                    log.info("[PERF] Cache HIT: {}ms, Key: {}", cacheTime, cacheKey);
                    cacheStatus = "hit";
                    return objectMapper.readValue(cachedValue, new TypeReference<List<Map<String, Object>>>() {
                    });
                }
            } catch (Exception e) {
                log.warn("Redis read failed: {}", e.getMessage());
                cacheStatus = "error";
            }

            long t2 = System.currentTimeMillis();
            log.info("[PERF] Cache MISS: {}ms", t2 - t1);

            // ========== 阶段 2: 指标扩展 ==========
            List<MetricDefinition> taskMetrics = expandMetrics(req.kpiArray(), req.includeHistoricalData());
            long t3 = System.currentTimeMillis();
            log.info("[PERF] ExpandMetrics: {}ms, Total: {} metrics", t3 - t2, taskMetrics.size());

            // ========== 阶段 3: 执行查询 ==========
            List<Map<String, Object>> finalResults = new ArrayList<>();
            for (String opTime : req.opTimeArray()) {
                long timeStart = System.currentTimeMillis();
                try {
                    List<Map<String, Object>> batchResults = executeSingleTimePoint(req, taskMetrics, opTime);
                    finalResults.addAll(batchResults);
                    long timeElapsed = System.currentTimeMillis() - timeStart;
                    log.info("[PERF] SingleTimePoint[{}]: {}ms, Rows: {}", opTime, timeElapsed, batchResults.size());
                } catch (Exception e) {
                    log.error("Query failed for opTime: {}", opTime, e);
                    throw new RuntimeException("Query failed for opTime: " + opTime, e);
                }
            }

            long t4 = System.currentTimeMillis();
            log.info("[PERF] AllTimePoints: {}ms, Total Rows: {}", t4 - t3, finalResults.size());

            // ========== 阶段 4: 缓存写入 ==========
            try {
                if (!finalResults.isEmpty() && redisCommands != null) {
                    String jsonResult = objectMapper.writeValueAsString(finalResults);
                    redisCommands.setex(cacheKey, cacheTtlMinutes * 60, jsonResult);
                    long cacheWriteTime = System.currentTimeMillis() - t4;
                    log.info("[PERF] CacheWrite: {}ms", cacheWriteTime);
                }
            } catch (Exception e) {
                log.warn("Redis write failed: {}", e.getMessage());
            }

            // ========== 总耗时 ==========
            long total = System.currentTimeMillis() - t0;
            log.info("[PERF] ===== TOTAL: {}ms (Cache={}ms, Expand={}ms, Query={}ms, Rows={}) =====",
                    total, t2 - t1, t3 - t2, t4 - t3, finalResults.size());

            return finalResults;

        } finally {
            sample.stop(Timer.builder("metrics.req.duration")
                    .description("API request duration")
                    .tag("cache", cacheStatus)
                    .register(registry));
        }
    }

    private List<Map<String, Object>> executeSingleTimePoint(
            KpiQueryRequest req,
            List<MetricDefinition> taskMetrics,
            String opTime) {

        long t0 = System.currentTimeMillis();

        QueryContext ctx = new QueryContext();
        ctx.setOpTime(opTime);
        ctx.setIncludeHistorical(req.includeHistoricalData());
        ctx.setIncludeTarget(req.includeTargetData());

        if (req.dimCodeArray() != null) {
            for (String dim : req.dimCodeArray()) {
                if (!dim.matches("^[a-zA-Z0-9_]+$")) {
                    throw new IllegalArgumentException("Invalid dimension code: " + dim);
                }
                ctx.addDimCode(dim);
            }
        }

        // ========== 子阶段 1: 依赖解析 ==========
        long t1 = System.currentTimeMillis();
        for (MetricDefinition metric : taskMetrics) {
            parser.resolveDependencies(metric, opTime, ctx);
        }
        long t2 = System.currentTimeMillis();
        log.info("[PERF]   Parse[{}]: {}ms, Tables: {}", opTime, t2 - t1, ctx.getRequiredTables().size());

        // ========== 子阶段 2: IO 准备 ==========
        preparePhysicalTables(ctx);
        long t3 = System.currentTimeMillis();
        log.info("[PERF]   Prepare[{}]: {}ms", opTime, t3 - t2);

        // ========== 子阶段 3: SQL 生成与执行 ==========
        int tableCount = ctx.getRequiredTables().size();
        List<String> dims = req.dimCodeArray() != null ? req.dimCodeArray() : new ArrayList<>();

        List<Map<String, Object>> results;
        long sqlGenStart = System.currentTimeMillis();

        if (tableCount > ATTACH_THRESHOLD) {
            log.info("[PERF]   UsingStagingMode (tables > {})", ATTACH_THRESHOLD);
            results = sqliteExecutor.executeWithStaging(ctx, dims,
                    (tableName) -> sqlGenerator.generateSqlWithStaging(taskMetrics, ctx, dims, tableName));
        } else {
            String sql = sqlGenerator.generateSql(taskMetrics, ctx, dims);
            long sqlGenEnd = System.currentTimeMillis();
            log.info("[PERF]   SqlGen[{}]: {}ms", opTime, sqlGenEnd - sqlGenStart);

            results = sqliteExecutor.executeQuery(ctx, sql);
        }

        long t4 = System.currentTimeMillis();
        log.info("[PERF]   SQLExec[{}]: {}ms, Rows: {}", opTime, t4 - sqlGenStart, results.size());

        results.forEach(row -> row.put("op_time", opTime));

        long total = System.currentTimeMillis() - t0;
        log.info("[PERF]   TimePoint[{}] TOTAL: {}ms", opTime, total);

        return results;
    }

    private void preparePhysicalTables(QueryContext ctx) {
        long t0 = System.currentTimeMillis();
        List<Callable<Void>> tasks = new ArrayList<>();

        // A. 下载 KPI 数据表
        int kpiTableCount = ctx.getRequiredTables().size();
        for (PhysicalTableReq req : ctx.getRequiredTables()) {
            tasks.add(() -> {
                long taskStart = System.currentTimeMillis();
                storageManager.downloadAndPrepare(req);
                String alias = "db_" + Math.abs((req.kpiId() + req.opTime()).hashCode());
                ctx.registerAlias(req, alias);
                long taskElapsed = System.currentTimeMillis() - taskStart;
                log.debug("[PERF]     DownloadKPI[{}_{}]: {}ms", req.kpiId(), req.opTime(), taskElapsed);
                return null;
            });
        }

        // B. 下载维度表
        Set<String> distinctCompDimCodes = ctx.getRequiredTables().stream()
                .map(PhysicalTableReq::compDimCode)
                .filter(Objects::nonNull)
                .collect(Collectors.toSet());

        for (String compDimCode : distinctCompDimCodes) {
            tasks.add(() -> {
                long taskStart = System.currentTimeMillis();
                String localPath = storageManager.downloadAndCacheDimDB(compDimCode);
                ctx.addDimensionTablePath(compDimCode, localPath);
                long taskElapsed = System.currentTimeMillis() - taskStart;
                log.debug("[PERF]     DownloadDim[{}]: {}ms", compDimCode, taskElapsed);
                return null;
            });
        }

        long t1 = System.currentTimeMillis();
        log.info("[PERF]     PrepareStart: KPI={}, Dim={}", kpiTableCount, distinctCompDimCodes.size());

        try {
            List<Future<Void>> futures = vThreadExecutor.invokeAll(tasks);
            for (Future<Void> f : futures)
                f.get();
        } catch (Exception e) {
            throw new RuntimeException("Failed to prepare tables", e);
        }

        long t2 = System.currentTimeMillis();
        log.info("[PERF]     PrepareComplete: {}ms (Parallel IO)", t2 - t1);
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
                baseDef = metadataRepo.findById(kpiInput);
            }
            tasks.add(baseDef);

            if (loadHistory && baseDef.type() != MetricType.VIRTUAL) {
                tasks.add(new MetricDefinition(baseDef.id() + "_lastYear", "${" + baseDef.id() + ".lastYear}",
                        MetricType.COMPOSITE, baseDef.aggFunc(), baseDef.compDimCode()));
                tasks.add(new MetricDefinition(baseDef.id() + "_lastCycle", "${" + baseDef.id() + ".lastCycle}",
                        MetricType.COMPOSITE, baseDef.aggFunc(), baseDef.compDimCode()));
            }
        }
        return tasks;
    }

    private String generateCacheKey(KpiQueryRequest req) {
        StringBuilder sb = new StringBuilder(CACHE_PREFIX);
        List<String> sortedKpis = new ArrayList<>(req.kpiArray());
        Collections.sort(sortedKpis);
        sb.append("kpis:").append(String.join(",", sortedKpis)).append("|");
        List<String> sortedTimes = new ArrayList<>(req.opTimeArray());
        Collections.sort(sortedTimes);
        sb.append("times:").append(String.join(",", sortedTimes)).append("|");
        if (req.dimCodeArray() != null && !req.dimCodeArray().isEmpty()) {
            List<String> sortedDims = new ArrayList<>(req.dimCodeArray());
            Collections.sort(sortedDims);
            sb.append("dims:").append(String.join(",", sortedDims)).append("|");
        }
        if (req.dimConditionArray() != null && !req.dimConditionArray().isEmpty()) {
            sb.append("conds:").append(req.dimConditionArray().hashCode()).append("|");
        }
        sb.append("hist:").append(req.includeHistoricalData()).append("|");
        sb.append("target:").append(req.includeTargetData());
        return sb.toString();
    }
}