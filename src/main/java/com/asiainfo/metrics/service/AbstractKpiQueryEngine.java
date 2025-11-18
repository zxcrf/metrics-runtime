package com.asiainfo.metrics.service;

import com.asiainfo.metrics.model.http.KpiQueryRequest;
import com.asiainfo.metrics.model.http.KpiQueryResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * KPI查询引擎抽象基类
 * 实现通用的查询流程和公共逻辑，具体引擎只需实现特定的查询逻辑
 *
 * 模板方法模式：定义了查询的核心流程，具体步骤由子类实现
 * @author QvQ
 * @date 2025/11/17
 */
public abstract class AbstractKpiQueryEngine implements KpiQueryEngine {

    private static final Logger log = LoggerFactory.getLogger(AbstractKpiQueryEngine.class);

    protected static final String NOT_EXISTS = "--";

    /**
     * 同步查询KPI数据 - 模板方法
     * 定义通用的查询流程，具体查询逻辑由子类实现
     *
     * @param request 查询请求
     * @return 查询结果
     */
    @Override
    public KpiQueryResult queryKpiData(KpiQueryRequest request) {
        // 1. 记录开始时间
        long startTime = System.currentTimeMillis();

        try {
            // 2. 参数验证
            validateRequest(request);

            // 3. 尝试从缓存获取结果（钩子方法，子类可实现）
            KpiQueryResult cachedResult = tryGetFromCache(request);
            if (cachedResult != null) {
                long elapsedTime = System.currentTimeMillis() - startTime;
                return buildQueryResult(cachedResult.dataArray(), elapsedTime, "查询成功 (缓存命中)");
            }
            try(Connection conn = getSQLiteConnection(request)){
                // 4. 查询准备（可选，子类可覆盖）
                preQuery(request, conn);

                // 5. 执行具体查询（子类必须实现）
                List<Map<String, Object>> flatResults = doQuery(request, conn);

                // 6. 结果聚合
                List<Map<String, Object>> aggregatedResults = aggregateResults(flatResults, request);

                // 7. 结果后处理（可选，子类可覆盖）
                postQuery(request, aggregatedResults, conn);

                // 8. 缓存结果（钩子方法，子类可实现）
                tryPutToCache(request, aggregatedResults);

                // 9. 构建最终结果
                long elapsedTime = System.currentTimeMillis() - startTime;
                return buildQueryResult(aggregatedResults, elapsedTime, "查询成功");
            }
        } catch (Exception e) {
            // 统一异常处理
            return handleQueryException(e);
        }
    }

    protected abstract Connection getSQLiteConnection(KpiQueryRequest request) throws SQLException, IOException;

    protected abstract String getKpiDataTableName(String kpiId, String cycleType, String compDimCode, String opTime);

    /**
     * 获取维度数据表名
     * @param compDimCode 组合维度编码
     * @return 维度数据表名
     */
    protected abstract String getDimDataTableName(String compDimCode);

        /**
         * 尝试从缓存获取结果
         * 子类可以覆盖此方法实现缓存逻辑
         *
         * @param request 查询请求
         * @return 缓存的查询结果，如果没有缓存则返回null
         */
    protected KpiQueryResult tryGetFromCache(KpiQueryRequest request) {
        // 默认不实现缓存
        return null;
    }

    /**
     * 尝试将结果存入缓存
     * 子类可以覆盖此方法实现缓存逻辑
     *
     * @param request 查询请求
     * @param aggregatedResults 聚合后的结果
     */
    protected void tryPutToCache(KpiQueryRequest request, List<Map<String, Object>> aggregatedResults) {
        // 默认不实现缓存
    }

    /**
     * 异步查询KPI数据 - 统一实现
     * 基于虚拟线程执行同步查询，实现统一的异步支持
     *
     * @param request 查询请求
     * @return 异步查询结果
     */
//    @Override
//    public CompletableFuture<KpiQueryResult> queryKpiDataAsync(KpiQueryRequest request) {
//        // 可以考虑注入配置的线程池
//        Executor executor = Executors.newVirtualThreadPerTaskExecutor();
//
//        return CompletableFuture.supplyAsync(() -> {
//            try {
//                return queryKpiData(request);
//            } catch (Exception e) {
//                throw new RuntimeException("异步查询失败", e);
//            }
//        }, executor);
//    }

    // ======================== 抽象方法 - 子类必须实现 ========================

    /**
     * 执行具体的KPI查询逻辑
     * @param request 查询请求
     * @return 扁平化的查询结果列表
     */
    protected abstract List<Map<String, Object>> doQuery(KpiQueryRequest request, Connection conn) throws Exception;

    // ======================== 钩子方法 - 子类可选覆盖 ========================

    /**
     * 查询准备工作
     * @param request 查询请求
     */
    protected void preQuery(KpiQueryRequest request, Connection conn) throws Exception {
        // 默认空实现，子类可覆盖
    }

    /**
     * 结果后处理
     * @param request 查询请求
     * @param aggregatedResults 聚合后的结果
     */
    protected void postQuery(KpiQueryRequest request, List<Map<String, Object>> aggregatedResults, Connection conn) throws Exception {
        // 默认空实现，子类可覆盖
    }

    // ======================== 通用方法 - 公共实现 ========================

    /**
     * 验证查询请求参数
     * @param request 查询请求
     */
    protected void validateRequest(KpiQueryRequest request) {
        if (request == null) {
            throw new IllegalArgumentException("查询请求不能为空");
        }
        if (request.kpiArray() == null || request.kpiArray().isEmpty()) {
            throw new IllegalArgumentException("KPI列表不能为空");
        }
        if (request.opTimeArray() == null || request.opTimeArray().isEmpty()) {
            throw new IllegalArgumentException("时间点列表不能为空");
        }
    }

    /**
     * 聚合查询结果
     * @param flatResults 扁平化结果
     * @param request 查询请求
     * @return 聚合后的结果
     */
    protected List<Map<String, Object>> aggregateResults(
            List<Map<String, Object>> flatResults, KpiQueryRequest request) {
        return KpiResultAggregator.aggregateResultsByDimensions(flatResults, request);
    }

    /**
     * 构建查询结果
     * @param data 聚合后的结果数据
     * @param elapsedTime 查询耗时
     * @param message 结果消息
     * @return 查询结果对象
     */
    protected KpiQueryResult buildQueryResult(List<Map<String, Object>> data, long elapsedTime, String message) {
        KpiQueryResult result = KpiQueryResult.success(data, message);
        // 可以添加统一的结果元数据
        return result;
    }

    /**
     * 处理查询异常
     * @param e 异常
     * @return 错误结果
     */
    protected KpiQueryResult handleQueryException(Exception e) {
        String errorMsg = "查询失败: " + e.getMessage();
        return KpiQueryResult.error(errorMsg);
    }

    protected boolean isComplexExpression(String expression) {
        return expression != null && expression.contains("${");
    }

    /**
     * 从复杂表达式中提取实际的KPI ID
     * 例如：${KD1002} -> KD1002, ${KD2001.lastCycle} -> KD2001
     */
    protected String extractActualKpiId(String expression) {
        if (!isComplexExpression(expression)) {
            return expression;
        }
        // 提取第一个KPI引用作为实际ID
        Pattern pattern = Pattern.compile("\\$\\{(K[DCYM]\\d{4})");
        Matcher matcher = pattern.matcher(expression);
        if (matcher.find()) {
            return matcher.group(1);
        }
        // 如果没有匹配到，返回原始表达式
        return expression;
    }

    /**
     * 计算上一周期时间
     */
    protected String calculateLastCycleTime(String currentOpTime) {
        try {
            LocalDate current = LocalDate.parse(currentOpTime, DateTimeFormatter.ofPattern("yyyyMMdd"));
            LocalDate lastCycle = current.minusMonths(1);
            return lastCycle.format(DateTimeFormatter.ofPattern("yyyyMMdd"));
        } catch (Exception e) {
            log.warn("计算上一周期时间失败，使用当前时间: {}", currentOpTime, e);
            return currentOpTime;
        }
    }

    /**
     * 计算去年同期时间
     */
    protected String calculateLastYearTime(String currentOpTime) {
        try {
            LocalDate current = LocalDate.parse(currentOpTime, DateTimeFormatter.ofPattern("yyyyMMdd"));
            LocalDate lastYear = current.minusYears(1);
            return lastYear.format(DateTimeFormatter.ofPattern("yyyyMMdd"));
        } catch (Exception e) {
            log.warn("计算去年同期时间失败，使用当前时间: {}", currentOpTime, e);
            return currentOpTime;
        }
    }




}
