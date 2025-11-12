package com.asiainfo.metrics.resource;

import com.asiainfo.metrics.config.MetricsConfig;
import com.asiainfo.metrics.model.ETLModel;
import com.asiainfo.metrics.service.KpiComputeService;
import com.asiainfo.metrics.service.KpiStorageService;
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
 * 指标计算与存储操作接口
 * 提供指标计算、存储的REST API
 *
 * @author QvQ
 * @date 2025/11/12
 */
@Path("/api/open/kpi")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class KpiOperationResource {

    private static final Logger log = LoggerFactory.getLogger(KpiOperationResource.class);

    @Inject
    KpiComputeService kpiComputeService;

    @Inject
    KpiStorageService kpiStorageService;

    @Inject
    MetricsConfig metricsConfig;

    /**
     * 源表数据完成触发器
     * 当源表数据准备好后，调用此接口触发派生指标的计算与存储
     *
     * @param etlModel ETL模型，包含源表名称和批次时间
     * @return 计算与存储结果
     */
    @POST
    @Path("/srcTableComplete")
    public Map<String, Object> startProduceExtendedMetrics(ETLModel etlModel) {
        log.info("收到源表完成通知，表名：{}，批次时间：{}", etlModel.tableName(), etlModel.opTime());

        Map<String, Object> result = new HashMap<>();

        try {
            // 1. 计算派生指标
            KpiComputeService.ComputeResult computeResult = kpiComputeService.computeExtendedMetrics(
                    etlModel.tableName(), etlModel.opTime().replaceAll("-", ""));

            if (!computeResult.success()) {
                result.put("status", "ERROR");
                result.put("message", computeResult.message());
                log.error("指标计算失败：{}", computeResult.message());
                return result;
            }

            log.info("指标计算成功，生成 {} 条数据", computeResult.data().size());

            // 2. 存储指标数据
            String engineType = metricsConfig.getCurrentEngine();
            KpiStorageService.StorageResult storageResult = kpiStorageService.storageMetrics(
                    computeResult.data(), engineType);

            if (!storageResult.success()) {
                result.put("status", "ERROR");
                result.put("message", storageResult.message());
                log.error("指标存储失败：{}", storageResult.message());
                return result;
            }

            log.info("指标存储成功，存储 {} 条记录", storageResult.storedCount());

            // 3. 返回成功结果
            result.put("status", "SUCCESS");
            result.put("message", "指标计算与存储成功");
            result.put("computeCount", computeResult.data().size());
            result.put("storageCount", storageResult.storedCount());
            result.put("engineType", engineType);
            result.put("tableName", etlModel.tableName());
            result.put("opTime", etlModel.opTime());

            log.info("指标计算与存储流程完成");

            return result;

        } catch (Exception e) {
            log.error("指标计算与存储过程发生异常", e);
            result.put("status", "ERROR");
            result.put("message", "发生异常：" + e.getMessage());
            return result;
        }
    }
}
