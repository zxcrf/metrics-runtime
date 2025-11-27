package com.asiainfo.metrics.v2.core.model;

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

    // 单次执行的时间切片 (保留兼容性)
    private String opTime;

    // 批量执行的所有时间切片 [新增]
    private List<String> targetOpTimes = Collections.emptyList();

    private boolean includeHistorical = false;
    private boolean includeTarget = false;

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

        if (alias == null) {
            throw new IllegalStateException(
                    String.format("Alias not found for %s. Ensure preparePhysicalTables() ran successfully.", key));
        }
        return alias;
    }

    public void addDimCode(String dimCode) {
        dimCodes.add(dimCode);
    }

    public List<String> getDimCodes() {
        return new ArrayList<>(dimCodes);
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
        opTime = null;
        targetOpTimes = Collections.emptyList();
        includeHistorical = false;
        includeTarget = false;
    }
}