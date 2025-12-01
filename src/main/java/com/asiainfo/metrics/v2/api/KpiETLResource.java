package com.asiainfo.metrics.v2.api;

import com.asiainfo.metrics.common.model.dto.ETLModel;
import com.asiainfo.metrics.v2.application.etl.ETLService;
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
        log.info("[V2] 收到源表完成通知，表名：{}，批次时间：{}", etlModel.tableName(), etlModel.opTime());

        Map<String, Object> result = new HashMap<>();

        try {
            // 执行 ETL 处理
            ETLService.ETLResult etlResult = etlService.processETL(
                    etlModel.tableName(), etlModel.opTime());

            if (!etlResult.success()) {
                result.put("status", "ERROR");
                result.put("message", etlResult.message());
                log.error("[V2] ETL 处理失败：{}", etlResult.message());
                return result;
            }

            // 返回成功结果
            result.put("status", "SUCCESS");
            result.put("message", "ETL 处理成功，数据已输出为 Parquet 格式");
            result.put("computeCount", etlResult.computeCount());
            result.put("storedCount", etlResult.storedCount());
            result.put("engineType", "V2-PARQUET");
            result.put("tableName", etlModel.tableName());
            result.put("opTime", etlModel.opTime());

            log.info("[V2] ETL 处理完成，计算 {} 条，存储 {} 条",
                    etlResult.computeCount(), etlResult.storedCount());

            return result;

        } catch (Exception e) {
            log.error("[V2] ETL 处理过程发生异常", e);
            result.put("status", "ERROR");
            result.put("message", "发生异常：" + e.getMessage());
            return result;
        }
    }
}
