package com.asiainfo.metrics.v2.infra.persistence;

import com.asiainfo.metrics.model.db.KpiDefinition;
import com.asiainfo.metrics.repository.KpiMetadataRepository;
import com.asiainfo.metrics.v2.core.model.MetricDefinition;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@ApplicationScoped
public class MetadataRepository {

    private static final Logger log = LoggerFactory.getLogger(MetadataRepository.class);
    private final Map<String, MetricDefinition> cache = new ConcurrentHashMap<>();

    @Inject
    KpiMetadataRepository legacyRepo;

    public MetricDefinition findById(String kpiId) {
        MetricDefinition cached = cache.get(kpiId);
        if (cached != null) return cached;

        try {
            KpiDefinition dbDef = legacyRepo.getKpiDefinition(kpiId);
            MetricDefinition def;

            if (dbDef != null) {
                def = convertToDomain(dbDef);
            } else {
                // 容错：如果没有定义，假设为默认物理指标，但 compDimCode 无法确定
                // 这里可以抛异常，或者给一个 'UNKNOWN'，但在实际工程中最好报错
                log.warn("Metric not found: {}, fallback to default", kpiId);
                // 假设默认 CD003，或者抛出异常
                throw new IllegalArgumentException("Metric not found: " + kpiId);
            }

            cache.put(kpiId, def);
            return def;

        } catch (Exception e) {
            throw new RuntimeException("Failed to load metadata for " + kpiId, e);
        }
    }

    private MetricDefinition convertToDomain(KpiDefinition dbDef) {
        String id = dbDef.kpiId();
        String dbType = dbDef.kpiType();
        String compDimCode = dbDef.compDimCode(); // 从 DB 获取
        String aggFunc = dbDef.aggFunc() == null || dbDef.aggFunc().isEmpty() ? "sum" : dbDef.aggFunc();

        if ("composite".equalsIgnoreCase(dbType)) {
            return MetricDefinition.composite(id, dbDef.kpiExpr(), aggFunc, compDimCode);
        } else if ("virtual".equalsIgnoreCase(dbType)) {
            // 数据库中定义的 Virtual 指标也可能有 compDimCode
            return MetricDefinition.virtual(id, dbDef.kpiExpr(), aggFunc);
        } else {
            // physical / extended
            return MetricDefinition.physical(id, aggFunc, compDimCode);
        }
    }

    public void clearCache() { cache.clear(); }
}