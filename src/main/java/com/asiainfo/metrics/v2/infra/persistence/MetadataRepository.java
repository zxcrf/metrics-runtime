package com.asiainfo.metrics.v2.infra.persistence;

import com.asiainfo.metrics.model.db.DimDef;
import com.asiainfo.metrics.repository.KpiMetadataRepository;
import com.asiainfo.metrics.v2.core.model.MetricDefinition;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

@ApplicationScoped
public class MetadataRepository {

    private static final Logger log = LoggerFactory.getLogger(MetadataRepository.class);
    private final Map<String, MetricDefinition> metricCache = new ConcurrentHashMap<>();

    // 新增缓存：记录每个 compDimCode 拥有的物理列名 (如 CD001 -> [city_id])
    private final Map<String, Set<String>> dimSchemaCache = new ConcurrentHashMap<>();

    @Inject
    KpiMetadataRepository legacyRepo;

    public MetricDefinition findById(String kpiId) {
        if (metricCache.containsKey(kpiId)) {
            // log.debug("Metadata Cache HIT: {}", kpiId);
            return metricCache.get(kpiId);
        }
        log.info("Metadata Cache MISS: {}", kpiId);
        return metricCache.computeIfAbsent(kpiId, this::loadMetricFromDb);
    }

    private MetricDefinition loadMetricFromDb(String kpiId) {
        log.info("Loading metric from DB: {}", kpiId);
        try {
            var dbDef = legacyRepo.getKpiDefinition(kpiId);
            if (dbDef != null) {
                log.info("Loaded metric from DB: {}", kpiId);
                return convertToDomain(dbDef);
            }
            // 容错：假设是物理指标，默认 CD003（生产环境建议抛异常）
            log.warn("Metric not found: {}", kpiId);
            throw new IllegalArgumentException("Metric not found: " + kpiId);
        } catch (Exception e) {
            log.error("Failed to load metadata for {}", kpiId, e);
            throw new RuntimeException("Failed to load metadata for " + kpiId, e);
        }
    }

    /**
     * 新增方法：获取组合维度包含的所有维度列名
     * 用于 SQL 生成时的字段对齐
     */
    public Set<String> getDimCols(String compDimCode) {
        return dimSchemaCache.computeIfAbsent(compDimCode, code -> {
            try {
                List<DimDef> dims = legacyRepo.getDimDefsByCompDim(code);
                if (dims == null || dims.isEmpty()) {
                    log.warn("No dimension definitions found for {}, using fallback defaults", code);
                    // 兜底策略，避免空指针
                    return Set.of("city_id", "county_id", "region_id");
                }
                return dims.stream()
                        .map(DimDef::dbColName)
                        .filter(Objects::nonNull)
                        .collect(Collectors.toSet());
            } catch (Exception e) {
                log.error("Failed to load schema for compDimCode: {}", code, e);
                return Collections.emptySet();
            }
        });
    }

    private MetricDefinition convertToDomain(com.asiainfo.metrics.model.db.KpiDefinition dbDef) {
        String id = dbDef.kpiId();
        String type = dbDef.kpiType();
        String compDim = dbDef.compDimCode();
        String agg = dbDef.aggFunc() == null || dbDef.aggFunc().isEmpty() ? "sum" : dbDef.aggFunc();
        String expr = dbDef.kpiExpr();

        if ("composite".equalsIgnoreCase(type)) {
            return MetricDefinition.composite(id, expr, agg, compDim);
        } else if ("virtual".equalsIgnoreCase(type)) {
            // 虚拟指标通常没有固定的 compDimCode
            return MetricDefinition.virtual(id, expr, agg);
        } else {
            return MetricDefinition.physical(id, agg, compDim);
        }
    }
}