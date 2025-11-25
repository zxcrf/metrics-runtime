package com.asiainfo.metrics.v2.core.model;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 查询上下文
 * 管理整个查询过程中的依赖关系、别名映射等信息
 */
public class QueryContext {
    private final Set<PhysicalTableReq> requiredTables = new HashSet<>();
    private final Map<PhysicalTableReq, String> dbAliasMap = new ConcurrentHashMap<>();
    private final Set<String> requiredDimCodes = new HashSet<>();
    private final List<String> dimCodes = new ArrayList<>();
    private String opTime;
    private String compDimCode;
    private boolean includeHistorical = false;
    private boolean includeTarget = false;

    /**
     * 添加需要的物理表
     */
    public void addPhysicalTable(String kpiId, String opTime, String compDimCode) {
        PhysicalTableReq req = new PhysicalTableReq(kpiId, opTime, compDimCode);
        requiredTables.add(req);
        requiredDimCodes.add(compDimCode);
    }

    /**
     * 获取所有需要的物理表
     */
    public Set<PhysicalTableReq> getRequiredTables() {
        return new HashSet<>(requiredTables);
    }

    /**
     * 注册数据库别名
     */
    public void registerAlias(PhysicalTableReq req, String alias) {
        dbAliasMap.put(req, alias);
    }

    /**
     * 获取数据库别名
     * 根据kpiId和opTime查找对应的别名
     */
    public String getAlias(String kpiId, String opTime) {
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
