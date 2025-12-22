package com.asiainfo.metrics.application.query;

import com.asiainfo.metrics.api.dto.KpiQueryRequest;
import com.asiainfo.metrics.domain.generator.SqlGenerator;
import com.asiainfo.metrics.domain.model.MetricDefinition;
import com.asiainfo.metrics.domain.model.MetricType;
import com.asiainfo.metrics.domain.model.PhysicalTableReq;
import com.asiainfo.metrics.domain.model.QueryContext;
import com.asiainfo.metrics.domain.parser.MetricParser;
import com.asiainfo.metrics.infrastructure.cache.CacheManager;
import com.asiainfo.metrics.infrastructure.persistence.DuckDBExecutor;
import com.asiainfo.metrics.infrastructure.persistence.MetadataRepository;
import com.asiainfo.metrics.infrastructure.storage.StorageManager;
import com.asiainfo.metrics.infrastructure.storage.StorageManager.FileNotExistException;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

@ApplicationScoped
public class UnifiedMetricEngine {

    private static final Logger log = LoggerFactory.getLogger(UnifiedMetricEngine.class);

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
    CacheManager cacheManager;

    private final java.util.concurrent.ExecutorService vThreadExecutor = Executors.newVirtualThreadPerTaskExecutor();

    public List<Map<String, Object>> execute(KpiQueryRequest req) {
        // 1. 尝试从缓存获取
        List<Map<String, Object>> result = cacheManager.get(req);
        if (result != null) {
            return result;
        }

        // 2. 执行查询
        result = executeBatchQuery(req);

        // 3. 写入缓存
        if (!result.isEmpty()) {
            cacheManager.put(req, result);
        }

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

        // 处理维度过滤条件
        // dimConditionVal 是逗号分隔的多值（OR关系），不同 dimConditionCode 之间是 AND 关系
        if (req.dimConditionArray() != null) {
            for (var cond : req.dimConditionArray()) {
                if (cond.dimConditionCode() != null && cond.dimConditionVal() != null) {
                    // 解析逗号分隔的值
                    List<String> values = Arrays.stream(cond.dimConditionVal().split(","))
                            .map(String::trim)
                            .filter(s -> !s.isEmpty())
                            .toList();
                    ctx.addDimCondition(cond.dimConditionCode(), values);
                }
            }
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
        // 传入 ctx 以便处理缺失数据
        results = restructureResults(results, taskMetrics, ctx);
        // long t3 = System.currentTimeMillis();

        // log.debug("[PERF] BatchExec: Parse={}ms, IO={}ms, Exec={}ms", t2-t1, t2-t1,
        // t3-t2);
        return results;
    }

    /**
     * 重构查询结果，将指标值放入 kpiValues map
     * 对于缺失数据的指标，填充 "--" (NOT_EXISTS)
     * 
     * 输出格式：kpiValues: { KD1006: {current: 123, lastCycle: "--", lastYear: "--"}, ... }
     * 
     * @param results 原始查询结果
     * @param metrics 查询的指标列表
     * @param ctx     查询上下文
     * @return 重构后的结果
     */
    private static final String NOT_EXISTS = "--";

    private List<Map<String, Object>> restructureResults(
            List<Map<String, Object>> results,
            List<MetricDefinition> metrics,
            QueryContext ctx) {

        if (results == null || results.isEmpty()) {
            return results;
        }

        List<String> dims = ctx.getDimCodes();

        // 获取所有指标ID（用于匹配）
        Set<String> allKpiIds = metrics.stream()
                .map(MetricDefinition::id)
                .collect(Collectors.toSet());

        // 维度相关的字段（维度本身 + 维度描述）
        Set<String> dimRelatedFields = new HashSet<>(dims.size() * 2);
        for (String dim : dims) {
            dimRelatedFields.add(dim);
            dimRelatedFields.add(dim + "_desc");
        }

        List<Map<String, Object>> restructured = new ArrayList<>(results.size());
        
        for (Map<String, Object> row : results) {
            Map<String, Object> newRow = new LinkedHashMap<>();
            Map<String, Object> kpiValues = new LinkedHashMap<>();

            for (Map.Entry<String, Object> entry : row.entrySet()) {
                String key = entry.getKey();
                Object value = entry.getValue();

                // opTime 保留在顶层
                if ("opTime".equals(key) || "op_time".equals(key)) {
                    newRow.put("opTime", value);
                    continue;
                }
                
                // 维度及其描述保留在顶层
                if (dimRelatedFields.contains(key)) {
                    newRow.put(key, value);
                    continue;
                }
                
                // 指标值分组处理
                if (allKpiIds.contains(key)) {
                    Object finalValue = value != null ? value : NOT_EXISTS;
                    
                    // 根据后缀确定 key 类型
                    if (key.endsWith("_lastCycle")) {
                        String baseId = key.substring(0, key.length() - 10); // "_lastCycle".length() = 10
                        getOrCreateKpiMap(kpiValues, baseId).put("lastCycle", finalValue);
                    } else if (key.endsWith("_lastYear")) {
                        String baseId = key.substring(0, key.length() - 9); // "_lastYear".length() = 9
                        getOrCreateKpiMap(kpiValues, baseId).put("lastYear", finalValue);
                    } else {
                        // 基础指标 -> current
                        getOrCreateKpiMap(kpiValues, key).put("current", finalValue);
                    }
                    continue;
                }
                
                // 其他未知字段（可能是虚拟指标表达式）
                kpiValues.put(key, value != null ? value : NOT_EXISTS);
            }

            if (!kpiValues.isEmpty()) {
                newRow.put("kpiValues", kpiValues);
            }
            restructured.add(newRow);
        }

        return restructured;
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> getOrCreateKpiMap(Map<String, Object> kpiValues, String kpiId) {
        return (Map<String, Object>) kpiValues.computeIfAbsent(kpiId, k -> new LinkedHashMap<>());
    }

    private void preparePhysicalTables(QueryContext ctx) {
        List<Callable<Void>> kpiTasks = new ArrayList<>();
        List<Callable<Void>> dimTasks = new ArrayList<>();

        // A. KPI Files - 每个任务需要关联对应的 PhysicalTableReq
        Map<Integer, PhysicalTableReq> taskIndexToReq = new java.util.concurrent.ConcurrentHashMap<>();
        int taskIndex = 0;
        for (PhysicalTableReq req : ctx.getRequiredTables()) {
            final int idx = taskIndex++;
            taskIndexToReq.put(idx, req);
            kpiTasks.add(() -> {
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
            dimTasks.add(() -> {
                String localPath = storageManager.downloadAndCacheDimDB(compDimCode);
                ctx.addDimensionTablePath(compDimCode, localPath);
                return null;
            });
        }

        try {
            // Parallel Download KPI files
            List<Future<Void>> kpiFutures = vThreadExecutor.invokeAll(kpiTasks);

            // 处理 KPI 下载结果，记录失败的到 context
            for (int i = 0; i < kpiFutures.size(); i++) {
                try {
                    kpiFutures.get(i).get();
                } catch (ExecutionException ee) {
                    Throwable cause = ee.getCause();
                    PhysicalTableReq req = taskIndexToReq.get(i);
                    
                    // 检查是否是文件不存在错误
                    if (cause instanceof FileNotExistException || isFileNotFoundError(cause)) {
                        // 记录缺失的表，继续处理其他文件
                        log.warn("[PrepareFiles] File not found: kpi={}, opTime={}, marking as missing",
                                req.kpiId(), req.opTime());
                        ctx.addMissingTable(req);
                    } else {
                        // 其他错误也记录为缺失，但记录更详细的日志
                        log.error("[PrepareFiles] Download failed for kpi={}, opTime={}: {}",
                                req.kpiId(), req.opTime(), cause.getMessage());
                        ctx.addMissingTable(req);
                    }
                }
            }

            // Parallel Download Dimension files (维度文件下载失败仍然抛出异常)
            List<Future<Void>> dimFutures = vThreadExecutor.invokeAll(dimTasks);
            for (Future<Void> f : dimFutures) {
                f.get(); // 维度文件是必需的，失败会抛出异常
            }

            if (ctx.hasMissingTables()) {
                log.info("[PrepareFiles] {} tables missing, proceeding with available data: {}",
                        ctx.getMissingTables().size(), ctx.getMissingTables());
            }

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Task interrupted", e);
        } catch (ExecutionException e) {
            throw new RuntimeException("Failed to prepare dimension tables", e.getCause());
        }
    }

    /**
     * 检查异常是否是文件不存在错误
     */
    private boolean isFileNotFoundError(Throwable t) {
        if (t == null) return false;
        String msg = t.getMessage();
        if (msg != null && (msg.contains("NoSuchKey") 
                || msg.contains("does not exist") 
                || msg.contains("Object does not exist"))) {
            return true;
        }
        return isFileNotFoundError(t.getCause());
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

    /**
     * 自定义异常：指标数据不存在
     */
    public static class MetricDataNotFoundException extends RuntimeException {
        private final List<String> missingMetrics;

        public MetricDataNotFoundException(List<String> missingMetrics) {
            super(buildMessage(missingMetrics));
            this.missingMetrics = new ArrayList<>(missingMetrics);
        }

        private static String buildMessage(List<String> missingMetrics) {
            if (missingMetrics.isEmpty()) {
                return "部分指标数据不存在";
            }
            return "以下指标数据不存在：" + String.join(", ", missingMetrics);
        }

        public List<String> getMissingMetrics() {
            return new ArrayList<>(missingMetrics);
        }

        public String getUserFriendlyMessage() {
            return getMessage();
        }
    }
}