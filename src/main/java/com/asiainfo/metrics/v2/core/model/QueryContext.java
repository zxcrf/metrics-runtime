package com.asiainfo.metrics.v2.core.model;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 查询上下文
 * 管理整个查询过程中的依赖关系、别名映射等信息
 */
public class QueryContext {
    // 必须使用线程安全集合，因为 preparePhysicalTables 是并发写入的
    private final Set<PhysicalTableReq> requiredTables = ConcurrentHashMap.newKeySet();
    private final Map<PhysicalTableReq, String> dbAliasMap = new ConcurrentHashMap<>();
    private final Set<String> dimCodes = ConcurrentHashMap.newKeySet();

    private String opTime; // 单次执行的时间上下文

    // 移除单一的 compDimCode，或者将其含义改为 "主维度表编码"
    // 如果业务逻辑强依赖这个字段去拼 kpi_dim_{code} 表名，建议改为由 SQL Generator 动态决定
    // private String compDimCode;

    /**
     * 添加需要的物理表
     */
    public void addPhysicalTable(String kpiId, String opTime, String compDimCode) {
        // compDimCode 记录在 TableReq 级别，而不是 Context 级别
        PhysicalTableReq req = new PhysicalTableReq(kpiId, opTime, compDimCode);
        requiredTables.add(req);
    }

    public void registerAlias(PhysicalTableReq req, String alias) {
        dbAliasMap.put(req, alias);
    }

    public String getAlias(String kpiId, String opTime) {
        // 这里的查找逻辑需要遍历 Key，稍微有点耗时但对于几十个表来说没问题
        // 为了性能，可以考虑加一个辅助 Map<String, String> key=id_time value=alias
        return dbAliasMap.entrySet().stream()
                .filter(e -> e.getKey().kpiId().equals(kpiId) && e.getKey().opTime().equals(opTime))
                .map(Map.Entry::getValue)
                .findFirst()
                .orElseThrow(() -> new RuntimeException("Alias not found for " + kpiId + "@" + opTime));
    }

    /**
     * 获取所有依赖的KPI ID集合
     */
    public Set<String> getRequiredKpiIds() {
        return requiredTables.stream()
                .map(PhysicalTableReq::kpiId)
                .collect(HashSet::new, Set::add, Set::addAll);
    }

    /**
     * 获取所有需要的时间点
     */
    public Set<String> getRequiredOpTimes() {
        return requiredTables.stream()
                .map(PhysicalTableReq::opTime)
                .collect(HashSet::new, Set::add, Set::addAll);
    }

    /**
     * 设置操作时间
     */
    public void setOpTime(String opTime) {
        this.opTime = opTime;
    }

    /**
     * 获取操作时间
     */
    public String getOpTime() {
        return opTime;
    }

    /**
     * 设置复合维度代码
     */
    public void setCompDimCode(String compDimCode) {
        this.compDimCode = compDimCode;
    }

    /**
     * 获取复合维度代码
     */
    public String getCompDimCode() {
        return compDimCode;
    }

    /**
     * 获取所有维度代码
     */
    public Set<String> getRequiredDimCodes() {
        return new HashSet<>(requiredDimCodes);
    }

    /**
     * 添加维度代码
     */
    public void addDimCode(String dimCode) {
        dimCodes.add(dimCode);
    }

    /**
     * 获取所有维度代码（来自查询请求）
     */
    public List<String> getDimCodes() {
        return new ArrayList<>(dimCodes);
    }

    /**
     * 设置是否包含历史数据
     */
    public void setIncludeHistorical(boolean includeHistorical) {
        this.includeHistorical = includeHistorical;
    }

    /**
     * 是否包含历史数据
     */
    public boolean isIncludeHistorical() {
        return includeHistorical;
    }

    /**
     * 设置是否包含目标值
     */
    public void setIncludeTarget(boolean includeTarget) {
        this.includeTarget = includeTarget;
    }

    /**
     * 是否包含目标值
     */
    public boolean isIncludeTarget() {
        return includeTarget;
    }

    /**
     * 计算上一年的时间
     * 例如：20251024 -> 20241024
     */
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

    /**
     * 清空所有状态
     */
    public void clear() {
        requiredTables.clear();
        dbAliasMap.clear();
        requiredDimCodes.clear();
        dimCodes.clear();
        opTime = null;
        compDimCode = null;
        includeHistorical = false;
        includeTarget = false;
    }
}
