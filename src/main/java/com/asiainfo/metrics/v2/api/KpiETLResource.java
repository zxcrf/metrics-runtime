package com.asiainfo.metrics.v2.api;

import com.asiainfo.metrics.common.model.dto.ETLModel;
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
     * 源表数据完成触发器 (V2)
     * 当源表数据准备好后，调用此接口触发派生指标的计算与 Parquet 生成
     *
     * @param etlModel ETL 模型，包含源表名称和批次时间
     * @return 计算与存储结果
     */
    @POST
    @Path("/srcTableComplete")
    @RunOnVirtualThread
    public Map<String, Object> triggerETL(ETLModel etlModel) {
        String kpiModelId = etlModel.tableName();
        String opTime = etlModel.opTime();
        log.info("[V2] ETL triggered: model={}, opTime={}", kpiModelId, opTime);

        Map<String, Object> result = new HashMap<>();

        // 1. 记录ETL开始
        Long executionId = executionLogger.logStart(kpiModelId, opTime, "system");
        long startTime = System.currentTimeMillis();

        try {
            // 2. 执行ETL处理
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

            // 4. 发布缓存失效（该模型在该时间点的所有派生指标）
            // TODO: 从模型配置中查询派生指标列表
            // cacheInvalidation.invalidateModelAndPublish(kpiModelId, derivedKpis,
            // List.of(opTime));

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
