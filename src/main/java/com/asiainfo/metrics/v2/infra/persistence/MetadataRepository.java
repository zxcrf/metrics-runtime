package com.asiainfo.metrics.v2.infra.persistence;

import com.asiainfo.metrics.model.db.KpiDefinition;
import com.asiainfo.metrics.repository.KpiMetadataRepository;
import com.asiainfo.metrics.v2.core.model.MetricDefinition;
import com.asiainfo.metrics.v2.core.model.MetricType;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 元数据仓库 (V2 Adapter)
 * 功能：
 * 1. 作为 V2 引擎与底层 MySQL 元数据的适配层。
 * 2. 提供高性能缓存 (ConcurrentHashMap)。
 * 3. 负责将数据库实体 (KpiDefinition) 转换为领域模型 (MetricDefinition)。
 */
@ApplicationScoped
public class MetadataRepository {

    private static final Logger log = LoggerFactory.getLogger(MetadataRepository.class);

    // 使用 ConcurrentHashMap 确保虚拟线程下的线程安全
    private final Map<String, MetricDefinition> cache = new ConcurrentHashMap<>();

    @Inject
    KpiMetadataRepository legacyRepo; // 注入现有的 MySQL 数据仓库

    /**
     * 根据KPI ID查找指标定义
     */
    public MetricDefinition findById(String kpiId) {
        // 1. 缓存优先 (Cache Hit)
        MetricDefinition cached = cache.get(kpiId);
        if (cached != null) {
            return cached;
        }

        // 2. 数据库查询 (Cache Miss)
        try {
            KpiDefinition dbDef = legacyRepo.getKpiDefinition(kpiId);
            MetricDefinition def;

            if (dbDef != null) {
                // 适配：DB实体 -> V2领域模型
                def = convertToDomain(dbDef);
                log.debug("Loaded KPI definition from DB: {}", kpiId);
            } else {
                // 3. 容错处理 (Fallback)
                // 如果数据库没找到，但文件系统可能存在（如临时物理表），降级为默认物理指标
                // 生产环境建议这里记录 WARN 日志
                log.warn("KPI definition not found in DB: {}, assuming default PHYSICAL", kpiId);
                def = MetricDefinition.physical(kpiId, "sum");
            }

            // 回填缓存
            cache.put(kpiId, def);
            return def;

        } catch (Exception e) {
            log.error("Failed to load KPI metadata: {}", kpiId, e);
            // 异常情况下抛出 RuntimeException 阻断流程，防止错误计算
            throw new RuntimeException("Failed to load metadata for " + kpiId, e);
        }
    }

    /**
     * 模型转换适配器
     */
    private MetricDefinition convertToDomain(KpiDefinition dbDef) {
        String id = dbDef.kpiId();
        String dbType = dbDef.kpiType(); // "composite", "extended", "physical"

        MetricType type;
        String expression;

        // 逻辑适配：根据 DB 中的类型字段映射到 V2 的枚举
        if ("composite".equalsIgnoreCase(dbType)) {
            type = MetricType.COMPOSITE;
            expression = dbDef.kpiExpr();
        } else if ("virtual".equalsIgnoreCase(dbType)) {
            type = MetricType.VIRTUAL;
            expression = dbDef.kpiExpr();
        } else {
            // "physical" 或 "extended" 都视为 V2 的物理原子指标
            // Extended (派生指标) 在 V2 引擎中通常由上层逻辑展开，底层仍是读取物理文件
            type = MetricType.PHYSICAL;
            // 物理指标的标准表达式：${ID.current}
            expression = "${" + id + ".current}";
        }

        // 处理聚合函数空值
        String aggFunc = dbDef.aggFunc();
        if (aggFunc == null || aggFunc.isEmpty()) {
            aggFunc = "sum";
        }

        return new MetricDefinition(id, expression, type, aggFunc);
    }

    /**
     * 清空缓存 (用于元数据变更通知)
     */
    public void clearCache() {
        cache.clear();
        log.info("Metadata cache cleared");
    }
}