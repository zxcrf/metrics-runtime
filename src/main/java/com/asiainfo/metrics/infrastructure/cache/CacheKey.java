package com.asiainfo.metrics.infrastructure.cache;

import com.asiainfo.metrics.api.dto.KpiQueryRequest;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * 缓存Key抽象
 * 统一管理不同层级的缓存Key格式
 */
public class CacheKey {

    private final List<String> kpiIds;
    private final List<String> opTimes;
    private final List<String> dimCodes;
    private final Boolean includeHistorical;
    private final CacheType type;

    public enum CacheType {
        QUERY_RESULT, // 查询结果缓存 (L1, L2)
        FILE_PATH // 文件路径缓存 (L3)
    }

    private CacheKey(List<String> kpiIds, List<String> opTimes,
            List<String> dimCodes, Boolean includeHistorical, CacheType type) {
        this.kpiIds = kpiIds != null ? new ArrayList<>(kpiIds) : new ArrayList<>();
        this.opTimes = opTimes != null ? new ArrayList<>(opTimes) : new ArrayList<>();
        this.dimCodes = dimCodes != null ? new ArrayList<>(dimCodes) : new ArrayList<>();
        this.includeHistorical = includeHistorical;
        this.type = type;
    }

    /**
     * 为查询请求创建缓存Key
     */
    public static CacheKey forQuery(KpiQueryRequest req) {
        return new CacheKey(
                req.kpiArray(),
                req.opTimeArray(),
                req.dimCodeArray(),
                req.includeHistoricalData(),
                CacheType.QUERY_RESULT);
    }

    /**
     * 为文件创建缓存Key
     */
    public static CacheKey forFile(String kpiId, String opTime, String compDimCode) {
        return new CacheKey(
                List.of(kpiId),
                List.of(opTime),
                List.of(compDimCode),
                null,
                CacheType.FILE_PATH);
    }

    /**
     * 生成L1缓存Key (Caffeine)
     * 格式:
     * metrics:v2:query:kpis:KD1001,KD1002|times:20251201|dims:city_id|hist:false
     */
    public String toL1Key() {
        List<String> sortedKpis = new ArrayList<>(kpiIds);
        Collections.sort(sortedKpis);

        List<String> sortedTimes = new ArrayList<>(opTimes);
        Collections.sort(sortedTimes);

        List<String> sortedDims = dimCodes != null ? new ArrayList<>(dimCodes) : new ArrayList<>();
        Collections.sort(sortedDims);

        StringBuilder sb = new StringBuilder("metrics:v2:query:");
        sb.append("kpis:").append(String.join(",", sortedKpis)).append("|");
        sb.append("times:").append(String.join(",", sortedTimes)).append("|");
        sb.append("dims:").append(String.join(",", sortedDims)).append("|");
        sb.append("hist:").append(includeHistorical);

        return sb.toString();
    }

    /**
     * 生成L2缓存Key (Redis)
     * 使用与L1相同的格式
     */
    public String toL2Key() {
        return toL1Key();
    }

    /**
     * 判断是否是文件类型的缓存
     */
    public boolean isFileType() {
        return type == CacheType.FILE_PATH;
    }

    public List<String> getKpiIds() {
        return new ArrayList<>(kpiIds);
    }

    public List<String> getOpTimes() {
        return new ArrayList<>(opTimes);
    }

    public List<String> getDimCodes() {
        return dimCodes != null ? new ArrayList<>(dimCodes) : new ArrayList<>();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        CacheKey cacheKey = (CacheKey) o;
        return Objects.equals(kpiIds, cacheKey.kpiIds) &&
                Objects.equals(opTimes, cacheKey.opTimes) &&
                Objects.equals(dimCodes, cacheKey.dimCodes) &&
                Objects.equals(includeHistorical, cacheKey.includeHistorical) &&
                type == cacheKey.type;
    }

    @Override
    public int hashCode() {
        return Objects.hash(kpiIds, opTimes, dimCodes, includeHistorical, type);
    }

    @Override
    public String toString() {
        return toL1Key();
    }
}
