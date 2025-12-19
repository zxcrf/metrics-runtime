package com.asiainfo.metrics.v2.api;

import com.asiainfo.metrics.common.model.dto.ETLModel;
import com.asiainfo.metrics.v2.api.dto.SrcTableCompleteRequest;
import com.asiainfo.metrics.v2.api.dto.SrcTableCompleteResponse;
import com.asiainfo.metrics.v2.application.etl.ETLExecutionLogger;
import com.asiainfo.metrics.v2.application.etl.ETLService;
import com.asiainfo.metrics.v2.infrastructure.cache.CacheInvalidationService;
import io.smallrye.common.annotation.RunOnVirtualThread;
import jakarta.inject.Inject;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * KPI ETL REST API (V2)
 * 提供 ETL 触发接口，直接输出 Parquet 到 MinIO
 *
 * @author QvQ
 * @date 2025/11/28
 */
@Path("/api/v2/kpi")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class KpiETLResource {

    private static final Logger log = LoggerFactory.getLogger(KpiETLResource.class);

    @Inject
    ETLService etlService;

    @Inject
    ETLExecutionLogger executionLogger;

    @Inject
    CacheInvalidationService cacheInvalidation;

    /**
     * 源表数据完成触发器 (V2 - 重构版)
     * 根据 FEAT_SRC_TABLE_COMPLETE.md 文档实现
     * 
     * 当源表数据准备好后，调用此接口：
     * 1. 记录表到达时间
     * 2. 查找依赖此表的所有模型
     * 3. 对每个模型检查依赖、执行并记录
     * 4. 发送 Webhook 通知
     *
     * @param request 包含 srcTableName（来源表名）和 opTime（批次号）
     * @return 处理结果，包含 triggeredModels、waitingModels、skippedModels
     */
    @POST
    @Path("/srcTableComplete")
    @RunOnVirtualThread
    public SrcTableCompleteResponse srcTableComplete(SrcTableCompleteRequest request) {
        String srcTableName = request.srcTableName();
        String opTime = request.opTime();

        log.info("[V2] srcTableComplete: srcTable={}, opTime={}", srcTableName, opTime);

        if (srcTableName == null || srcTableName.isBlank()) {
            return SrcTableCompleteResponse.error("srcTableName 不能为空");
        }
        if (opTime == null || opTime.isBlank()) {
            return SrcTableCompleteResponse.error("opTime 不能为空");
        }

        try {
            return etlService.handleTableArrival(srcTableName, opTime);
        } catch (Exception e) {
            log.error("[V2] srcTableComplete failed", e);
            return SrcTableCompleteResponse.error("处理失败: " + e.getMessage());
        }
    }

    /**
     * 旧版触发器（保持向后兼容）
     * 直接根据模型ID执行ETL，不走表到达流程
     *
     * @param etlModel ETL 模型，tableName 实际为 modelId
     * @return 计算与存储结果
     * @deprecated 请使用新版 srcTableComplete 接口
     */
    @POST
    @Path("/triggerETL")
    @RunOnVirtualThread
    @Deprecated
    public Map<String, Object> triggerETL(ETLModel etlModel) {
        String kpiModelId = etlModel.tableName();
        String opTime = etlModel.opTime();
        log.info("[V2] Legacy triggerETL: model={}, opTime={}", kpiModelId, opTime);

        Map<String, Object> result = new HashMap<>();

        // 1. 记录ETL开始
        Long executionId = executionLogger.logStart(kpiModelId, opTime, "system");
        long startTime = System.currentTimeMillis();

        try {
            // 2. 执行ETL处理（使用旧版逻辑）
            ETLService.ETLResult etlResult = etlService.processETL(kpiModelId, opTime);
            long executionTime = System.currentTimeMillis() - startTime;

            if (!etlResult.success()) {
                // 记录失败
                executionLogger.logFailure(executionId, etlResult.message());

                result.put("status", "ERROR");
                result.put("message", etlResult.message());
                log.error("[V2] ETL failed: {}", etlResult.message());
                return result;
            }

            // 3. 记录成功
            executionLogger.logSuccess(executionId, etlResult.storedCount(), executionTime);

            log.info("[V2] ETL completed: model={}, opTime={}, records={}, time={}ms",
                    kpiModelId, opTime, etlResult.storedCount(), executionTime);

            // 返回成功结果
            result.put("status", "SUCCESS");
            result.put("message", "ETL completed successfully");
            result.put("computeCount", etlResult.computeCount());
            result.put("storedCount", etlResult.storedCount());
            result.put("executionTimeMs", executionTime);
            result.put("tableName", kpiModelId);
            result.put("opTime", opTime);
            result.put("executionId", executionId);

            return result;

        } catch (Exception e) {
            // 记录异常
            executionLogger.logFailure(executionId, e.getMessage());

            log.error("[V2] ETL exception", e);
            result.put("status", "ERROR");
            result.put("message", "Exception: " + e.getMessage());
            return result;
        }
    }
}
