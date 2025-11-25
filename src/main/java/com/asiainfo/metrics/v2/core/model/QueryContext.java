package com.asiainfo.metrics.v2.core.model;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 查询上下文 (Thread-Safe Refactored)
 * 适配虚拟线程并发写入场景
 */
public class QueryContext {
    // 使用并发集合，支持多线程 add
    private final Set<PhysicalTableReq> requiredTables = ConcurrentHashMap.newKeySet();
    private final Map<PhysicalTableReq, String> dbAliasMap = new ConcurrentHashMap<>();
    private final Set<String> dimCodes = ConcurrentHashMap.newKeySet(); // 维度代码集合

    // 单次执行的时间切片
    private String opTime;
    // 主维度编码 (如果存在混合维度，具体使用哪个由 PhysicalTableReq 决定)
    private String compDimCode;

    private boolean includeHistorical = false;
    private boolean includeTarget = false;

    public void addPhysicalTable(String kpiId, String opTime, String compDimCode) {
        PhysicalTableReq req = new PhysicalTableReq(kpiId, opTime, compDimCode);
        requiredTables.add(req);
    }

    public Set<PhysicalTableReq> getRequiredTables() {
        return requiredTables; // 返回并发集合视图
    }

    public void registerAlias(PhysicalTableReq req, String alias) {
        dbAliasMap.put(req, alias);
    }

    public String getAlias(String kpiId, String opTime) {
        // 查找逻辑：遍历 Entry (由于 Map 大小通常 < 50，性能可接受)
        // 如果需要极致性能，可增加辅助 Map<String, String> key=id_time value=alias
        return dbAliasMap.entrySet().stream()
                .filter(e -> e.getKey().kpiId().equals(kpiId) && e.getKey().opTime().equals(opTime))
                .map(Map.Entry::getValue)
                .findFirst()
                .orElseThrow(() -> new IllegalStateException(
                        String.format("Alias not found for %s@%s. Ensure preparePhysicalTables() ran successfully.", kpiId, opTime)));
    }

    public void addDimCode(String dimCode) {
        dimCodes.add(dimCode);
    }

    public List<String> getDimCodes() {
        return new ArrayList<>(dimCodes);
    }

    // --- Getters & Setters ---

    public void setOpTime(String opTime) { this.opTime = opTime; }
    public String getOpTime() { return opTime; }

    public void setCompDimCode(String compDimCode) { this.compDimCode = compDimCode; }
    public String getCompDimCode() { return compDimCode; }

    public void setIncludeHistorical(boolean includeHistorical) { this.includeHistorical = includeHistorical; }
    public boolean isIncludeHistorical() { return includeHistorical; }

    public void setIncludeTarget(boolean includeTarget) { this.includeTarget = includeTarget; }
    public boolean isIncludeTarget() { return includeTarget; }

    // 保留原有的日期计算辅助方法...
    public String getLastYearTime() {
        // 简化的实现示例，实际应复用 Engine 或 Util 的逻辑
        if (opTime == null || opTime.length() != 8) return opTime;
        int year = Integer.parseInt(opTime.substring(0, 4)) - 1;
        return year + opTime.substring(4);
    }
}