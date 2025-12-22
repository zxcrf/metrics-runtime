package com.asiainfo.metrics.api;

import com.asiainfo.metrics.api.dto.SrcTableCompleteRequest;
import com.asiainfo.metrics.api.dto.SrcTableCompleteResponse;
import com.asiainfo.metrics.application.etl.ETLService;
import io.smallrye.common.annotation.RunOnVirtualThread;
import jakarta.inject.Inject;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * KPI ETL REST API
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

    /**
     * 源表数据完成触发器
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

        log.info("[ETL] srcTableComplete: srcTable={}, opTime={}", srcTableName, opTime);

        if (srcTableName == null || srcTableName.isBlank()) {
            return SrcTableCompleteResponse.error("srcTableName 不能为空");
        }
        if (opTime == null || opTime.isBlank()) {
            return SrcTableCompleteResponse.error("opTime 不能为空");
        }

        try {
            return etlService.handleTableArrival(srcTableName, opTime);
        } catch (Exception e) {
            log.error("[ETL] srcTableComplete failed", e);
            return SrcTableCompleteResponse.error("处理失败: " + e.getMessage());
        }
    }
}
