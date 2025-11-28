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

    @Inject
    MetricParser parser;
    @Inject
    SqlGenerator sqlGenerator;
    @Inject
    DuckDBExecutor duckdbExecutor;
    @Inject
    MetadataRepository metadataRepo;
    @Inject
    StorageManager storageManager;
    @Inject
    RedisDataSource redisDataSource;
    @Inject
    ObjectMapper objectMapper;
    // @Inject MeterRegistry registry;

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
        // long t0 = System.currentTimeMillis();
        String cacheKey = generateCacheKey(req);

        // 1. L1 Cache (Caffeine)
        List<Map<String, Object>> localResult = localCache.getIfPresent(cacheKey);
        if (localResult != null) {
            // log.debug("[PERF] L1 Cache HIT: {}ms", System.currentTimeMillis() - t0);
            return localResult;
        }

        // 2. L2 Cache (Redis)
        try {
            ValueCommands<String, String> redis = redisDataSource.value(String.class);
            String cachedValue = redis.get(cacheKey);
            if (cachedValue != null) {
                List<Map<String, Object>> result = objectMapper.readValue(cachedValue, new TypeReference<>() {
                });
                localCache.put(cacheKey, result); // 回填 L1
                // log.debug("[PERF] L2 Cache HIT: {}ms", System.currentTimeMillis() - t0);
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

        // log.debug("[PERF] Query Total: {}ms, Rows: {}", System.currentTimeMillis() -
        // t0, result.size());
        return result;
    }

    private List<Map<String, Object>> executeBatchQuery(KpiQueryRequest req) {
        // long t0 = System.currentTimeMillis();

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
        // long t1 = System.currentTimeMillis();
        for (String opTime : req.opTimeArray()) {
            for (MetricDefinition metric : taskMetrics) {
                parser.resolveDependencies(metric, opTime, ctx);
            }
        }

        // 4. Batch Prepare Files (Parallel IO)
        preparePhysicalTables(ctx);
        // long t2 = System.currentTimeMillis();

        // 5. Generate Batch SQL (Single Large Query)
        String sql = sqlGenerator.generateBatchSql(ctx);

        log.debug("[SQL Generation] Generated SQL length: {}, Context metrics: {}, Required tables: {}",
                sql.length(), taskMetrics.size(), ctx.getRequiredTables().size());

        if (sql.isEmpty()) {
            log.warn("[SQL Generation] Empty SQL generated! Metrics: {}, Context: {}",
                    taskMetrics.stream().map(m -> m.id() + ":" + m.expression()).toList(),
                    ctx.getRequiredTables());
            return Collections.emptyList();
        }

        // 6. Execute DuckDB
        List<Map<String, Object>> results = duckdbExecutor.executeQuery(ctx, sql);

        // 7. Restructure results: move KPI values to kpiValues map
        results = restructureResults(results, taskMetrics, ctx.getDimCodes());
        // long t3 = System.currentTimeMillis();

        // log.debug("[PERF] BatchExec: Parse={}ms, IO={}ms, Exec={}ms", t2-t1, t2-t1,
        // t3-t2);
        return results;
    }

    /**
     * 重构查询结果，将指标值放入 kpiValues map
     * 
     * @param results 原始查询结果
     * @param metrics 查询的指标列表
     * @param dims    维度列表
     * @return 重构后的结果
     */
    private List<Map<String, Object>> restructureResults(
            List<Map<String, Object>> results,
            List<MetricDefinition> metrics,
            List<String> dims) {

        if (results == null || results.isEmpty()) {
            return results;
        }

        // 获取所有指标的ID
        Set<String> kpiIds = metrics.stream()
                .map(m -> m.id())
                .collect(java.util.stream.Collectors.toSet());

        // 维度相关的字段（维度本身 + 维度描述）
        Set<String> dimRelatedFields = new HashSet<>();
        for (String dim : dims) {
            dimRelatedFields.add(dim);
            dimRelatedFields.add(dim + "_desc");
        }

        List<Map<String, Object>> restructured = new ArrayList<>();
        for (Map<String, Object> row : results) {
            Map<String, Object> newRow = new LinkedHashMap<>();
            Map<String, Object> kpiValues = new LinkedHashMap<>();

            for (Map.Entry<String, Object> entry : row.entrySet()) {
                String key = entry.getKey();
                Object value = entry.getValue();

                // opTime 保留在顶层
                if ("opTime".equals(key) || "op_time".equals(key)) {
                    newRow.put("opTime", value);
                }
                // 维度及其描述保留在顶层
                else if (dimRelatedFields.contains(key)) {
                    newRow.put(key, value);
                }
                // 指标值放入 kpiValues
                else if (kpiIds.contains(key)) {
                    kpiValues.put(key, value);
                }
                // 其他未知字段也放入kpiValues（可能是虚拟指标）
                else {
                    kpiValues.put(key, value);
                }
            }

            // 只有当有kpi值时才添加kpiValues字段
            if (!kpiValues.isEmpty()) {
                newRow.put("kpiValues", kpiValues);
            }

            restructured.add(newRow);
        }

        return restructured;
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
            for (Future<Void> f : futures)
                f.get();
        } catch (Exception e) {
            throw new RuntimeException("Failed to prepare tables", e);
        }
    }

    private List<MetricDefinition> expandMetrics(List<String> kpiArray, Boolean includeHistorical) {
        List<MetricDefinition> tasks = new ArrayList<>();
        boolean loadHistory = Boolean.TRUE.equals(includeHistorical);

        for (String kpiInput : kpiArray) {
            MetricDefinition baseDef;

            // 1. 检查是否是虚拟表达式（包含 ${ 或者包含运算符 +,-,*,/,(,) ）
            if (isVirtualExpression(kpiInput)) {
                // 使用表达式本身作为ID（清理特殊字符）
                // String id = generateVirtualMetricId(kpiInput);
                baseDef = MetricDefinition.virtual("'" + kpiInput + "'", kpiInput, "sum");
                // log.debug("Recognized virtual expression: '{}' as {}", kpiInput, id);
            }
            // 2. 检查是否是 kpiId.timeModifier 格式 (如 KC8001.lastYear)
            else if (kpiInput.contains(".") && !kpiInput.contains("${")) {
                String[] parts = kpiInput.split("\\.", 2);
                String kpiId = parts[0];
                String modifier = parts.length > 1 ? parts[1] : "current";

                // 构造完整表达式
                String expression = "${" + kpiId + "." + modifier + "}";
                MetricDefinition physicalDef = metadataRepo.findById(kpiId);
                baseDef = new MetricDefinition(
                        kpiId + "_" + modifier,
                        expression,
                        MetricType.COMPOSITE,
                        physicalDef.aggFunc(),
                        physicalDef.compDimCode());
                log.debug("Expanded shorthand notation: '{}' to expression: '{}'", kpiInput, expression);
            }
            // 3. 普通指标ID，从数据库加载
            else {
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

    /**
     * 判断是否是虚拟表达式
     * 包含以下任一特征即为虚拟表达式：
     * 1. 包含 ${ 语法
     * 2. 包含运算符 +, -, *, /
     * 3. 包含括号 (, )
     */
    private boolean isVirtualExpression(String input) {
        if (input == null || input.isEmpty()) {
            return false;
        }

        // 包含 ${ 语法
        if (input.contains("${")) {
            return true;
        }

        // 包含运算符或括号（排除点号，点号用于 kpiId.modifier 格式）
        return input.matches(".*[+\\-*/()].*");
    }

    /**
     * 生成虚拟指标ID
     * 使用表达式本身，但移除特殊字符以确保SQL兼容性
     */
    private String generateVirtualMetricId(String expression) {
        if (expression == null || expression.isEmpty()) {
            return "V_EXPR";
        }

        // 如果表达式较短（<= 30个字符），直接使用清理后的表达式
        // 否则使用表达式的前20个字符 + hashcode
        if (expression.length() <= 30) {
            // 移除特殊字符，保留字母数字和下划线
            return expression.replaceAll("[^a-zA-Z0-9_.]", "_");
        } else {
            // 长表达式：使用前缀 + hashcode
            String prefix = expression.substring(0, 20).replaceAll("[^a-zA-Z0-9_.]", "_");
            return prefix + "_" + Math.abs(expression.hashCode());
        }
    }
}