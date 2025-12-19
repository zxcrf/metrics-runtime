package com.asiainfo.metrics.v2.domain.model;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 查询上下文 (Thread-Safe Refactored)
 * 适配虚拟线程并发写入场景，支持批量时间点上下文
 */
public class QueryContext {
    // 使用并发集合，支持多线程 add
    private final Set<PhysicalTableReq> requiredTables = ConcurrentHashMap.newKeySet();
    private final Set<String> dimCodes = ConcurrentHashMap.newKeySet(); // 维度代码集合
    private final Map<String, String> fastAliasIndex = new ConcurrentHashMap<>();
    private final Map<String, String> dimensionTablePaths = new ConcurrentHashMap<>();
    
    // 记录下载失败的表（kpiId@opTime -> 错误信息）
    private final Set<String> missingTables = ConcurrentHashMap.newKeySet();

    // 单次执行的时间切片 (保留兼容性)
    private String opTime;

    // 批量执行的所有时间切片 [新增]
    private List<String> targetOpTimes = Collections.emptyList();

    private boolean includeHistorical = false;
    private boolean includeTarget = false;
    
    // 维度过滤条件: dimCode -> 允许的值列表 (多个值之间是 OR 关系)
    private final Map<String, List<String>> dimConditions = new ConcurrentHashMap<>();

    private List<MetricDefinition> metrics = Collections.emptyList();

    public void setMetrics(List<MetricDefinition> metrics) {
        this.metrics = metrics;
    }

    public List<MetricDefinition> getMetrics() {
        return metrics;
    }

    public void addPhysicalTable(String kpiId, String opTime, String compDimCode) {
        PhysicalTableReq req = new PhysicalTableReq(kpiId, opTime, compDimCode);
        requiredTables.add(req);
    }

    private final Map<String, String> dimensionAliases = new ConcurrentHashMap<>();

    public void addDimensionTablePath(String compDimCode, String path) {
        dimensionTablePaths.put(compDimCode, path);
    }

    public void registerDimensionAlias(String compDimCode, String alias) {
        dimensionAliases.put(compDimCode, alias);
    }

    public String getDimensionAlias(String compDimCode) {
        return dimensionAliases.get(compDimCode);
    }

    public Map<String, String> getDimensionTablePaths() {
        return dimensionTablePaths;
    }

    public Set<PhysicalTableReq> getRequiredTables() {
        return requiredTables; // 返回并发集合视图
    }

    public void registerAlias(PhysicalTableReq req, String alias) {
        fastAliasIndex.put(req.kpiId() + "@" + req.opTime(), alias);
    }

    public String getAlias(String kpiId, String opTime) {
        // 优化：O(1) 直接查找
        String key = kpiId + "@" + opTime;
        String alias = fastAliasIndex.get(key);

        if (alias == null && !missingTables.contains(key)) {
            throw new IllegalStateException(
                    String.format("Alias not found for %s. Ensure preparePhysicalTables() ran successfully.", key));
        }
        // 如果在 missingTables 中，返回 null 表示缺失
        return alias;
    }

    public void addDimCode(String dimCode) {
        dimCodes.add(dimCode);
    }

    public List<String> getDimCodes() {
        return new ArrayList<>(dimCodes);
    }

    // --- Missing Tables 管理 ---

    /**
     * 标记某个表下载失败
     */
    public void addMissingTable(PhysicalTableReq req) {
        missingTables.add(req.kpiId() + "@" + req.opTime());
    }

    /**
     * 检查某个指标在某个时间点是否缺失
     */
    public boolean isMissing(String kpiId, String opTime) {
        return missingTables.contains(kpiId + "@" + opTime);
    }

    /**
     * 是否有任何缺失的表
     */
    public boolean hasMissingTables() {
        return !missingTables.isEmpty();
    }

    /**
     * 获取所有缺失的表
     */
    public Set<String> getMissingTables() {
        return new java.util.HashSet<>(missingTables);
    }

    // --- Getters & Setters ---

    public void setOpTime(String opTime) {
        this.opTime = opTime;
    }

    public String getOpTime() {
        return opTime;
    }

    public void setTargetOpTimes(List<String> targetOpTimes) {
        this.targetOpTimes = targetOpTimes;
    }

    public List<String> getTargetOpTimes() {
        return targetOpTimes;
    }

    public void setIncludeHistorical(boolean includeHistorical) {
        this.includeHistorical = includeHistorical;
    }

    public boolean isIncludeHistorical() {
        return includeHistorical;
    }

    public void setIncludeTarget(boolean includeTarget) {
        this.includeTarget = includeTarget;
    }

    public boolean isIncludeTarget() {
        return includeTarget;
    }

    /**
     * 添加维度过滤条件
     * @param dimCode 维度代码，如 "city_id"
     * @param values 允许的值列表，如 ["C0030", "C0031"]
     */
    public void addDimCondition(String dimCode, List<String> values) {
        if (dimCode != null && values != null && !values.isEmpty()) {
            dimConditions.put(dimCode, new ArrayList<>(values));
        }
    }

    /**
     * 获取所有维度过滤条件
     */
    public Map<String, List<String>> getDimConditions() {
        return dimConditions;
    }

    /**
     * 是否有维度过滤条件
     */
    public boolean hasDimConditions() {
        return !dimConditions.isEmpty();
    }

    /**
     * 计算上一周期的时间（减1个月）
     * 依赖 opTime 字段，仅用于单点计算场景，批量场景请使用 Parser 工具类
     */
    public String getLastCycleTime() {
        if (opTime == null || opTime.length() != 8) {
            // 如果没有设置单点时间，尝试取列表第一个，或者抛异常
            throw new IllegalArgumentException("OpTime not set in context for relative calculation.");
        }
        String year = opTime.substring(0, 4);
        String monthStr = opTime.substring(4, 6);
        String day = opTime.substring(6, 8);

        int yearInt = Integer.parseInt(year);
        int monthInt = Integer.parseInt(monthStr);

        // 上一月
        if (monthInt == 1) {
            monthInt = 12;
            yearInt--;
        } else {
            monthInt--;
        }

        return String.format("%d%02d%s", yearInt, monthInt, day);
    }

    public void clear() {
        requiredTables.clear();
        dimCodes.clear();
        fastAliasIndex.clear();
        dimensionTablePaths.clear();
        dimensionAliases.clear();
        missingTables.clear();
        dimConditions.clear();
        opTime = null;
        targetOpTimes = Collections.emptyList();
        includeHistorical = false;
        includeTarget = false;
    }
}