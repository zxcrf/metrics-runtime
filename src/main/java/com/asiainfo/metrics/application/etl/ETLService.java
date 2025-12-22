package com.asiainfo.metrics.application.etl;

import com.asiainfo.metrics.infrastructure.compute.KpiComputeService.KpiDataRecord;
import com.asiainfo.metrics.infrastructure.persistence.KpiDataSourceRepository;
import com.asiainfo.metrics.infrastructure.persistence.KpiMetadataRepository;
import com.asiainfo.metrics.domain.model.KpiDefinition;
import com.asiainfo.metrics.domain.model.KpiModel;
import com.asiainfo.metrics.api.dto.ModelExecutionResult;
import com.asiainfo.metrics.api.dto.SrcTableCompleteResponse;
import com.asiainfo.metrics.application.webhook.WebhookNotificationService;
import com.asiainfo.metrics.infrastructure.persistence.ETLLogRepository;
import com.asiainfo.metrics.infrastructure.persistence.ModelDependencyRepository;
import com.asiainfo.metrics.infrastructure.persistence.TaskLogRepository;
import com.asiainfo.metrics.infrastructure.persistence.TaskLogRepository.TaskLog;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.time.LocalDateTime;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * ETL 服务 (V2 - 重构版)
 * 根据 FEAT_SRC_TABLE_COMPLETE.md 技术文档实现
 * 
 * 核心功能：
 * 1. 表到达记录（UPSERT语义）
 * 2. 多依赖检查
 * 3. ETL重跑检测
 * 4. 并发控制
 * 5. 流式读写防止OOM
 *
 * @author QvQ
 * @date 2025/12/19
 */
@ApplicationScoped
public class ETLService {

    private static final Logger log = LoggerFactory.getLogger(ETLService.class);

    @Inject
    ETLLogRepository etlLogRepository;

    @Inject
    ModelDependencyRepository dependencyRepository;

    @Inject
    TaskLogRepository taskLogRepository;

    @Inject
    KpiMetadataRepository metadataRepository;

    @Inject
    KpiDataSourceRepository dataSourceRepository;

    @Inject
    ParquetWriter parquetWriter;

    @Inject
    WebhookNotificationService webhookService;

    /**
     * 核心入口：处理来源表到达
     * 根据 FEAT_SRC_TABLE_COMPLETE.md 文档实现
     */
    public SrcTableCompleteResponse handleTableArrival(String srcTableName, String opTime) {
        log.info("[ETL] Handling table arrival: srcTable={}, opTime={}", srcTableName, opTime);

        try {
            // 1. 记录/更新表到达时间（UPSERT）
            etlLogRepository.logTableArrival(srcTableName, opTime);

            // 2. 查找依赖此表的所有模型
            List<String> dependentModels = dependencyRepository.findModelsByDependencyTableName(srcTableName);

            if (dependentModels.isEmpty()) {
                log.info("[ETL] No models depend on table: {}", srcTableName);
                return SrcTableCompleteResponse.ignored("无模型依赖此表");
            }

            log.info("[ETL] Found {} models depending on table: {}", dependentModels.size(), srcTableName);

            // 3. 处理每个模型
            List<ModelExecutionResult> triggeredModels = new ArrayList<>();
            List<String> waitingModels = new ArrayList<>();
            List<String> skippedModels = new ArrayList<>();

            for (String modelId : dependentModels) {
                try {
                    processModel(modelId, opTime, triggeredModels, waitingModels, skippedModels);
                } catch (Exception e) {
                    log.error("[ETL] Error processing model: {}", modelId, e);
                    triggeredModels.add(ModelExecutionResult.failed(modelId, opTime, e.getMessage()));
                }
            }

            log.info("[ETL] Table arrival processed: triggered={}, waiting={}, skipped={}",
                    triggeredModels.size(), waitingModels.size(), skippedModels.size());

            return SrcTableCompleteResponse.success(triggeredModels, waitingModels, skippedModels);

        } catch (Exception e) {
            log.error("[ETL] Failed to handle table arrival", e);
            return SrcTableCompleteResponse.error("处理失败: " + e.getMessage());
        }
    }

    /**
     * 处理单个模型
     */
    private void processModel(String modelId, String opTime,
                              List<ModelExecutionResult> triggeredModels,
                              List<String> waitingModels,
                              List<String> skippedModels) {

        // 检查模型是否已发布
        if (!isModelPublished(modelId)) {
            log.info("[ETL] Model not published, skipping: {}", modelId);
            skippedModels.add(modelId);
            return;
        }

        // 检查所有依赖是否满足
        if (!isModelReady(modelId, opTime)) {
            log.info("[ETL] Model dependencies not ready, waiting: {}", modelId);
            waitingModels.add(modelId);
            return;
        }

        // 检查是否需要重跑
        if (!shouldReExecute(modelId, opTime)) {
            log.info("[ETL] Model doesn't need re-execution, skipping: {}", modelId);
            skippedModels.add(modelId);
            return;
        }

        // 执行并记录
        ModelExecutionResult result = executeModelWithLogging(modelId, opTime);
        triggeredModels.add(result);
    }

    /**
     * 检查模型是否已发布 (state=1)
     */
    private boolean isModelPublished(String modelId) {
        KpiModel model = metadataRepository.getModelById(modelId);
        if (model == null) {
            log.warn("[ETL] Model not found: {}", modelId);
            return false;
        }
        return "1".equals(model.state());
    }

    /**
     * 检查模型的所有依赖表是否已到达
     */
    private boolean isModelReady(String modelId, String opTime) {
        List<String> dependencies = dependencyRepository.getDependenciesForModel(modelId);

        if (dependencies.isEmpty()) {
            // 无依赖配置，默认认为就绪
            log.debug("[ETL] No dependencies configured for model: {}", modelId);
            return true;
        }

        for (String tableName : dependencies) {
            if (!etlLogRepository.exists(tableName, opTime)) {
                log.debug("[ETL] Dependency not ready: model={}, table={}", modelId, tableName);
                return false;
            }
        }

        return true;
    }

    /**
     * 检查是否需要重新执行（ETL重跑检测）
     * 规则：如果任意依赖表的 arrival_time > 上次成功执行的 end_time，则需要重新执行
     */
    private boolean shouldReExecute(String modelId, String opTime) {
        Optional<TaskLog> lastSuccess = taskLogRepository.findLastSuccessTask(modelId, opTime);

        if (lastSuccess.isEmpty()) {
            // 从未执行过
            log.debug("[ETL] Never executed before: model={}, opTime={}", modelId, opTime);
            return true;
        }

        LocalDateTime lastEndTime = lastSuccess.get().endTime();
        if (lastEndTime == null) {
            return true;
        }

        // 检查所有依赖表的到达时间
        List<String> dependencies = dependencyRepository.getDependenciesForModel(modelId);

        for (String tableName : dependencies) {
            Optional<LocalDateTime> arrivalTime = etlLogRepository.getArrivalTime(tableName, opTime);
            if (arrivalTime.isPresent() && arrivalTime.get().isAfter(lastEndTime)) {
                log.info("[ETL] ETL re-run detected: model={}, table={}, arrival={}, lastEnd={}",
                        modelId, tableName, arrivalTime.get(), lastEndTime);
                return true;
            }
        }

        log.debug("[ETL] No re-execution needed: model={}, opTime={}", modelId, opTime);
        return false;
    }

    /**
     * 执行模型并记录日志
     * 使用流式读写防止OOM
     */
    private ModelExecutionResult executeModelWithLogging(String modelId, String opTime) {
        log.info("[ETL] Executing model: {}, opTime={}", modelId, opTime);

        // 并发控制：检查是否有正在执行的任务
        if (taskLogRepository.existsRunningTask(modelId, opTime)) {
            log.info("[ETL] Task already running, skipping: model={}, opTime={}", modelId, opTime);
            return ModelExecutionResult.skipped(modelId, opTime, "任务正在执行中");
        }

        // 创建 RUNNING 状态的任务日志
        Long taskId = taskLogRepository.createRunningTask(modelId, opTime);

        try {
            // 使用流式处理执行ETL
            StreamingETLResult result = executeETLStreaming(modelId, opTime);

            // 更新任务为成功
            taskLogRepository.markSuccess(taskId, result.computeCount(), result.storageCount());

            // 发送 Webhook 通知
            if (!result.extendedKpiIds().isEmpty()) {
                webhookService.notifyKpiUpdated(modelId, opTime, result.extendedKpiIds());
            }

            return ModelExecutionResult.success(modelId, opTime, result.computeCount(), result.storageCount());

        } catch (Exception e) {
            log.error("[ETL] Model execution failed: model={}", modelId, e);
            taskLogRepository.markFailed(taskId, e.getMessage());
            return ModelExecutionResult.failed(modelId, opTime, e.getMessage());
        }
    }

    /**
     * 流式ETL结果
     */
    private record StreamingETLResult(int computeCount, int storageCount, List<String> extendedKpiIds) {}

    /**
     * 执行流式 ETL
     * 使用 JDBC 边读边写，防止大量数据导致 OOM
     */
    private StreamingETLResult executeETLStreaming(String modelId, String opTime) throws Exception {
        log.info("[ETL] Starting streaming ETL: model={}, opTime={}", modelId, opTime);

        // 获取模型定义
        KpiModel model = metadataRepository.getModelById(modelId);
        if (model == null) {
            throw new RuntimeException("模型不存在: " + modelId);
        }

        // 获取派生指标
        List<KpiDefinition> extendedKpis = metadataRepository.getExtendedKpisByModelId(modelId);
        if (extendedKpis.isEmpty()) {
            throw new RuntimeException("未找到需要抽取的派生指标");
        }

        List<String> extendedKpiIds = extendedKpis.stream()
                .map(KpiDefinition::kpiId)
                .collect(Collectors.toList());

        // 处理表名模板（替换日期占位符）
        String tableName = extractTableNameFromModelSql(model.modelSql());
        String realTableName = replaceTableNameTemplate(tableName, opTime);

        // 构建抽取SQL
        String extractSql = buildExtractSql(model, extendedKpis, opTime, tableName, realTableName);
        log.info("[ETL] Extract SQL:\n{}", extractSql);

        // 流式读取并写入
        int computeCount = 0;
        int storageCount = 0;

        // 用于收集批量写入的数据
        List<KpiDataRecord> batch = new ArrayList<>();
        int batchSize = 1000; // 每批处理1000条

        try (Connection conn = dataSourceRepository.getConnection(model.modelDsName());
             PreparedStatement stmt = conn.prepareStatement(extractSql,
                     ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)) {

            // 设置流式读取（MySQL）
            stmt.setFetchSize(Integer.MIN_VALUE);

            try (ResultSet rs = stmt.executeQuery()) {
                Set<String> kpiIds = extendedKpis.stream()
                        .map(KpiDefinition::kpiId)
                        .collect(Collectors.toSet());

                while (rs.next()) {
                    computeCount++;

                    // 将每行转换为多条KpiDataRecord（每个指标一条）
                    List<KpiDataRecord> records = convertRowToRecords(rs, extendedKpis, opTime, kpiIds);
                    batch.addAll(records);

                    // 批量写入
                    if (batch.size() >= batchSize) {
                        ParquetWriter.WriteResult writeResult = parquetWriter.writeToParquet(batch);
                        storageCount += writeResult.storedCount();
                        batch.clear();
                        log.debug("[ETL] Batch written, computed={}, stored={}", computeCount, storageCount);
                    }
                }

                // 写入剩余数据
                if (!batch.isEmpty()) {
                    ParquetWriter.WriteResult writeResult = parquetWriter.writeToParquet(batch);
                    storageCount += writeResult.storedCount();
                }
            }
        }

        log.info("[ETL] Streaming ETL completed: model={}, compute={}, storage={}",
                modelId, computeCount, storageCount);

        return new StreamingETLResult(computeCount, storageCount, extendedKpiIds);
    }

    /**
     * 从模型SQL中提取表名
     */
    private String extractTableNameFromModelSql(String modelSql) {
        // 简单实现：查找 FROM 后的表名
        Pattern pattern = Pattern.compile("(?i)FROM\\s+([\\w_]+)");
        Matcher matcher = pattern.matcher(modelSql);
        if (matcher.find()) {
            return matcher.group(1);
        }
        return "";
    }

    /**
     * 替换表名中的日期模板
     */
    private String replaceTableNameTemplate(String tableName, String opTime) {
        opTime = opTime.replace("-", "");
        String regex = "(?i)(yyyymmdd|yyyymm|yyyy)";
        Pattern pattern = Pattern.compile(regex);
        Matcher matcher = pattern.matcher(tableName);
        if (matcher.find()) {
            return matcher.replaceAll(opTime);
        }
        return tableName;
    }

    /**
     * 构建抽取SQL
     */
    private String buildExtractSql(KpiModel model, List<KpiDefinition> kpis, String opTime,
                                   String tableName, String realTableName) {
        // 获取维度字段
        String dimFields = metadataRepository.getDimFieldsStringByCompDim(model.compDimCode());

        // 替换占位符
        String sql = model.modelSql();
        if (sql.contains(tableName) && !tableName.equals(realTableName)) {
            sql = sql.replace(tableName, realTableName);
        }

        StringBuilder finalSql = new StringBuilder("SELECT op_time, ");
        finalSql.append(dimFields);

        StringBuilder metricsExpr = new StringBuilder();
        for (int i = 0; i < kpis.size(); i++) {
            if (i != 0) {
                metricsExpr.append(",");
            }
            KpiDefinition kpi = kpis.get(i);
            metricsExpr.append(kpi.kpiExpr()).append(" AS ").append(kpi.kpiId());
            finalSql.append(", ").append(kpi.kpiExpr()).append(" AS ").append(kpi.kpiId());
        }

        sql = sql.replace("${op_time}", opTime);
        sql = sql.replace("${dimGroup}", dimFields);
        // sql = sql.replace("${metrics_def}", metricsExpr.toString());

        finalSql.append(" FROM (").append(sql).append(") t\n");
        finalSql.append(" GROUP BY op_time, ").append(dimFields);

        return finalSql.toString();
    }

    /**
     * 将ResultSet的一行转换为多条KpiDataRecord
     */
    private List<KpiDataRecord> convertRowToRecords(ResultSet rs, List<KpiDefinition> kpis,
                                                     String opTime, Set<String> kpiIds) throws Exception {
        List<KpiDataRecord> records = new ArrayList<>();

        // 提取维度值
        Map<String, Object> dimValues = new LinkedHashMap<>();
        var meta = rs.getMetaData();
        for (int i = 1; i <= meta.getColumnCount(); i++) {
            String colName = meta.getColumnLabel(i);
            if (!kpiIds.contains(colName) && !"op_time".equalsIgnoreCase(colName)) {
                dimValues.put(colName, rs.getObject(i));
            }
        }

        // 为每个指标创建一条记录
        for (KpiDefinition kpi : kpis) {
            Object kpiVal = rs.getObject(kpi.kpiId());
            if (kpiVal != null) {
                records.add(new KpiDataRecord(
                        kpi.kpiId(),
                        opTime,
                        kpi.compDimCode(),
                        new LinkedHashMap<>(dimValues),
                        kpiVal
                ));
            }
        }

        return records;
    }
}
