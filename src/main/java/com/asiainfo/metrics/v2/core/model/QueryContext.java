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
//    private final Map<PhysicalTableReq, String> dbAliasMap = new ConcurrentHashMap<>();
    private final Set<String> dimCodes = ConcurrentHashMap.newKeySet(); // 维度代码集合
    private final Map<String, String> fastAliasIndex = new ConcurrentHashMap<>();

    // 单次执行的时间切片
    private String opTime;

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
//        dbAliasMap.put(req, alias);
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

    public void setOpTime(String opTime) { this.opTime = opTime; }
    public String getOpTime() { return opTime; }


    public void setIncludeHistorical(boolean includeHistorical) { this.includeHistorical = includeHistorical; }
    public boolean isIncludeHistorical() { return includeHistorical; }

    public void setIncludeTarget(boolean includeTarget) { this.includeTarget = includeTarget; }
    public boolean isIncludeTarget() { return includeTarget; }

    public String getLastYearTime() {
        if (opTime == null || opTime.length() != 8) {
            throw new IllegalArgumentException("Invalid opTime format, expected yyyyMMdd: " + opTime);
        }
        String year = opTime.substring(0, 4);
        String monthDay = opTime.substring(4);
        int lastYear = Integer.parseInt(year) - 1;
        return String.format("%d%s", lastYear, monthDay);
    }

    /**
     * 计算上一周期的时间（减1个月）
     * 例如：20251024 -> 20250924
     */
    public String getLastCycleTime() {
        if (opTime == null || opTime.length() != 8) {
            throw new IllegalArgumentException("Invalid opTime format, expected yyyyMMdd: " + opTime);
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
//        dbAliasMap.clear();
//        requiredDimCodes.clear();
        dimCodes.clear();
        opTime = null;
//        compDimCode = null;
        includeHistorical = false;
        includeTarget = false;
    }
}