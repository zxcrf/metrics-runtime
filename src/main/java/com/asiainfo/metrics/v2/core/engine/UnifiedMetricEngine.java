package com.asiainfo.metrics.v2.core.engine;

import com.asiainfo.metrics.model.http.KpiQueryRequest;
import com.asiainfo.metrics.v2.core.generator.SqlGenerator;
import com.asiainfo.metrics.v2.core.model.MetricDefinition;
import com.asiainfo.metrics.v2.core.model.MetricType;
import com.asiainfo.metrics.v2.core.model.PhysicalTableReq;
import com.asiainfo.metrics.v2.core.model.QueryContext;
import com.asiainfo.metrics.v2.core.parser.MetricParser;
import com.asiainfo.metrics.v2.infra.persistence.DuckDBExecutor;
import com.asiainfo.metrics.v2.infra.persistence.MetadataRepository;
import com.asiainfo.metrics.v2.infra.storage.StorageManager;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import io.quarkus.redis.datasource.RedisDataSource;
import io.quarkus.redis.datasource.value.ValueCommands;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

@ApplicationScoped
public class UnifiedMetricEngine {

    private static final Logger log = LoggerFactory.getLogger(UnifiedMetricEngine.class);
    private static final String CACHE_PREFIX = "metrics:v2:query:";

    @Inject MetricParser parser;
    @Inject SqlGenerator sqlGenerator;
    @Inject DuckDBExecutor duckdbExecutor;
    @Inject MetadataRepository metadataRepo;
    @Inject StorageManager storageManager;
    @Inject RedisDataSource redisDataSource;
    @Inject ObjectMapper objectMapper;
//    @Inject MeterRegistry registry;

    @ConfigProperty(name = "kpi.cache.ttl.minutes", defaultValue = "30")
    long cacheTtlMinutes;

    // L1 Cache: 5秒热点缓存，抗突发流量
    private final Cache<String, List<Map<String, Object>>> localCache = Caffeine.newBuilder()
            .expireAfterWrite(5, TimeUnit.SECONDS)
            .maximumSize(10_000)
            .recordStats()
            .build();

    private final ExecutorService vThreadExecutor = Executors.newVirtualThreadPerTaskExecutor();

    public List<Map<String, Object>> execute(KpiQueryRequest req) {
//        long t0 = System.currentTimeMillis();
        String cacheKey = generateCacheKey(req);

        // 1. L1 Cache (Caffeine)
        List<Map<String, Object>> localResult = localCache.getIfPresent(cacheKey);
        if (localResult != null) {
//            log.debug("[PERF] L1 Cache HIT: {}ms", System.currentTimeMillis() - t0);
            return localResult;
        }

        // 2. L2 Cache (Redis)
        try {
            ValueCommands<String, String> redis = redisDataSource.value(String.class);
            String cachedValue = redis.get(cacheKey);
            if (cachedValue != null) {
                List<Map<String, Object>> result = objectMapper.readValue(cachedValue, new TypeReference<>() {});
                localCache.put(cacheKey, result); // 回填 L1
//                log.debug("[PERF] L2 Cache HIT: {}ms", System.currentTimeMillis() - t0);
                return result;
            }
        } catch (Exception e) {
            log.warn("Redis error: {}", e.getMessage());
        }

        // 3. Execute Query (Batch Mode)
        List<Map<String, Object>> result = executeBatchQuery(req);

        // 4. Write Back Cache
        if (!result.isEmpty()) {
            localCache.put(cacheKey, result);
            vThreadExecutor.submit(() -> {
                try {
                    redisDataSource.value(String.class).setex(cacheKey, cacheTtlMinutes * 60,
                            objectMapper.writeValueAsString(result));
                } catch (Exception e) {
                    log.warn("Redis write failed", e);
                }
            });
        }

//        log.debug("[PERF] Query Total: {}ms, Rows: {}", System.currentTimeMillis() - t0, result.size());
        return result;
    }

    private List<Map<String, Object>> executeBatchQuery(KpiQueryRequest req) {
//        long t0 = System.currentTimeMillis();

        // 1. Expand Metrics
        List<MetricDefinition> taskMetrics = expandMetrics(req.kpiArray(), req.includeHistoricalData());

        // 2. Build Context
        QueryContext ctx = new QueryContext();
        ctx.setIncludeHistorical(req.includeHistoricalData());
        ctx.setIncludeTarget(req.includeTargetData());
        ctx.setTargetOpTimes(req.opTimeArray());
        ctx.setMetrics(taskMetrics);

        if (req.dimCodeArray() != null) {
            req.dimCodeArray().forEach(ctx::addDimCode);
        }

        // 3. Batch Resolve Dependencies
        // 针对所有请求的时间点，解析所有指标的依赖
//        long t1 = System.currentTimeMillis();
        for (String opTime : req.opTimeArray()) {
            for (MetricDefinition metric : taskMetrics) {
                parser.resolveDependencies(metric, opTime, ctx);
            }
        }

        // 4. Batch Prepare Files (Parallel IO)
        preparePhysicalTables(ctx);
//        long t2 = System.currentTimeMillis();

        // 5. Generate Batch SQL (Single Large Query)
        String sql = sqlGenerator.generateBatchSql(ctx);

        if (sql.isEmpty()) return Collections.emptyList();

        // 6. Execute DuckDB
        List<Map<String, Object>> results = duckdbExecutor.executeQuery(ctx, sql);
//        long t3 = System.currentTimeMillis();

//        log.debug("[PERF] BatchExec: Parse={}ms, IO={}ms, Exec={}ms", t2-t1, t2-t1, t3-t2);
        return results;
    }

    private void preparePhysicalTables(QueryContext ctx) {
        List<Callable<Void>> tasks = new ArrayList<>();

        // A. KPI Files
        for (PhysicalTableReq req : ctx.getRequiredTables()) {
            tasks.add(() -> {
                // downloadAndPrepare now returns absolute path to parquet
                String localPath = storageManager.downloadAndPrepare(req);
                // 注册为路径，供 SqlGenerator 使用
                ctx.registerAlias(req, localPath);
                return null;
            });
        }

        // B. Dimension Files
        Set<String> distinctCompDimCodes = ctx.getRequiredTables().stream()
                .map(PhysicalTableReq::compDimCode)
                .filter(Objects::nonNull)
                .collect(Collectors.toSet());

        for (String compDimCode : distinctCompDimCodes) {
            tasks.add(() -> {
                String localPath = storageManager.downloadAndCacheDimDB(compDimCode);
                ctx.addDimensionTablePath(compDimCode, localPath);
                return null;
            });
        }

        try {
            // Parallel Download
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

        if (req.dimCodeArray() != null) {
            List<String> sortedDims = new ArrayList<>(req.dimCodeArray());
            Collections.sort(sortedDims);
            sb.append("dims:").append(String.join(",", sortedDims)).append("|");
        }
        sb.append("hist:").append(req.includeHistoricalData());
        return sb.toString();
    }
}