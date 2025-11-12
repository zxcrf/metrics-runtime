package com.asiainfo.metrics.service;

import com.asiainfo.metrics.model.KpiDefinition;
import com.asiainfo.metrics.model.KpiModel;
import com.asiainfo.metrics.repository.KpiDataSourceRepository;
import com.asiainfo.metrics.repository.KpiMetadataRepository;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * 指标计算服务
 * 负责从源表抽取数据并计算派生指标
 *
 * @author QvQ
 * @date 2025/11/12
 */
@ApplicationScoped
public class KpiComputeService {

    private static final Logger log = LoggerFactory.getLogger(KpiComputeService.class);

    @Inject
    KpiMetadataRepository metadataRepository;

    @Inject
    KpiDataSourceRepository kpiDataSourceRepository;

    /**
     * 计算指定批次的所有派生指标
     *
     * @param tableName 源表名称
     * @param opTime    批次时间
     * @return 计算结果
     */
    public ComputeResult computeExtendedMetrics(String tableName, String opTime) {
        try {
            log.info("开始计算派生指标，源表：{}，批次：{}", tableName, opTime);

            // 1. 获取源表对应的取数模型
            KpiModel modelDef = metadataRepository.getMetricsModelDef(tableName);
            if (modelDef == null) {
                log.error("未找到源表 {} 的取数模型", tableName);
                return ComputeResult.error("未找到取数模型: " + tableName);
            }

            // 2. 获取该批次需要计算的所有派生指标
//            String compDimCode = metadataRepository.getCompDimCodeByTableName(tableName);
            List<KpiDefinition> extendedKpis = metadataRepository.getExtendedKpisByCycleAndDim(
                    modelDef.compDimCode(), getCycleType(opTime));
            if (extendedKpis.isEmpty()) {
                log.warn("未找到需要计算的派生指标");
                return ComputeResult.error("未找到需要计算的派生指标");
            }

            log.info("找到 {} 个需要计算的派生指标", extendedKpis.size());

            // 3. 拼接取数SQL
            String computeSql = buildComputeSql(modelDef, extendedKpis, opTime);
            log.info("计算SQL：\n{}", computeSql);

            // 4. 执行计算
            List<Map<String, Object>> results = executeComputeSql(modelDef.modelDsName(), computeSql);

            if (results.isEmpty()) {
                log.warn("计算结果为空");
                return ComputeResult.error("计算结果为空");
            }

            log.info("计算完成，生成 {} 条原始数据", results.size());

            // 5. 转换为纵表格式（每个指标一行）
            List<KpiDataRecord> records = convertToVerticalFormat(results, extendedKpis, opTime);

            log.info("转换为纵表格式，共 {} 条指标数据", records.size());

            return ComputeResult.success(records);

        } catch (Exception e) {
            log.error("计算派生指标失败", e);
            return ComputeResult.error("计算失败: " + e.getMessage());
        }
    }

    /**
     * 根据批次时间获取周期类型
     */
    private String getCycleType(String opTime) {
        if (opTime.length() == 8) {
            return "DAY";
        } else if (opTime.length() == 6) {
            return "MONTH";
        } else if (opTime.length() == 4) {
            return "YEAR";
        }
        return "DAY";
    }

    /**
     * 构建计算SQL
     */
    private String buildComputeSql(KpiModel modelDef, List<KpiDefinition> kpis, String opTime) {
        // 拼接指标表达式
        StringBuilder metricsExpr = new StringBuilder();
        for (KpiDefinition kpi : kpis) {
            // 派生指标的kpiExpr是SQL片段，直接使用
            metricsExpr.append(", ").append(kpi.kpiExpr()).append(" as ").append(kpi.kpiId());
        }

        // 获取维度字段（从组合维度编码解析）
        String dimFields = getDimFieldsFromCompDimCode(modelDef.compDimCode());

        // 替换占位符
        String sql = modelDef.modelSql();
        sql = sql.replace("${op_time}", "'" + opTime + "'");
        sql = sql.replace("${dimGroup}", dimFields);
        sql = sql.replace("${metrics_def}", metricsExpr.toString());

        return sql;
    }

    /**
     * 从组合维度编码获取维度字段列表
     * 从数据库动态获取，不再硬编码
     */
    private String getDimFieldsFromCompDimCode(String compDimCode) {
        return metadataRepository.getDimFieldsStringByCompDim(compDimCode);
    }

    /**
     * 执行计算SQL
     */
    private List<Map<String, Object>> executeComputeSql(String dsName, String sql) throws SQLException {
        List<Map<String, Object>> resultList = new ArrayList<>();

        try (Connection conn = kpiDataSourceRepository.getConnection(dsName);
             PreparedStatement stmt = conn.prepareStatement(sql);
             ResultSet rs = stmt.executeQuery()) {

             KpiQueryEngine.sqlRowMapping(resultList, rs);
        }

        return resultList;
    }

    /**
     * 将横表结果转换为纵表格式
     * 原始：{op_time, city_id, KD1001, KD1002, KD1003}
     * 目标：[{kpi_id, op_time, city_id, kpi_val}, ...]
     */
    private List<KpiDataRecord> convertToVerticalFormat(
            List<Map<String, Object>> results,
            List<KpiDefinition> kpis,
            String opTime) {

        List<KpiDataRecord> records = new ArrayList<>();

        for (Map<String, Object> row : results) {
            for (KpiDefinition kpi : kpis) {
                String kpiId = kpi.kpiId();
                Object kpiValObj = row.get(kpiId);

                if (kpiValObj != null) {
                    KpiDataRecord record = new KpiDataRecord(
                            kpiId,
                            opTime,
                            kpi.compDimCode(),
                            row,
                            kpiValObj
                    );
                    records.add(record);
                }
            }
        }

        return records;
    }

    /**
     * 计算结果
     */
    public record ComputeResult(boolean success, List<KpiDataRecord> data, String message) {

        public static ComputeResult success(List<KpiDataRecord> data) {
            return new ComputeResult(true, data, null);
        }

        public static ComputeResult error(String message) {
            return new ComputeResult(false, null, message);
        }
    }

    /**
     * 指标数据记录
     */
    public record KpiDataRecord(String kpiId, String opTime, String compDimCode, Map<String, Object> dimValues,
                                Object kpiVal) {
    }
}
