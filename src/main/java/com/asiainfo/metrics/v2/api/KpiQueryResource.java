package com.asiainfo.metrics.v2.api;

import com.asiainfo.metrics.model.http.KpiQueryRequest;
import com.asiainfo.metrics.model.http.KpiQueryResult;
import com.asiainfo.metrics.v2.core.engine.UnifiedMetricEngine;
import io.smallrye.common.annotation.RunOnVirtualThread;
import jakarta.inject.Inject;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;

import java.util.List;
import java.util.Map;

/**
 * KPI查询REST API
 * 提供HTTP接口用于查询KPI数据
 */
@Path("/api/v2/kpi")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class KpiQueryResource {

    @Inject
    UnifiedMetricEngine engine;

    /**
     * 查询KPI数据（与v1接口完全一致）
     *
     * @param request 查询请求
     * @return 查询结果（格式与v1完全一致：dataArray, status, msg）
     */
    @POST
    @Path("/queryKpiData")
    @RunOnVirtualThread
    public KpiQueryResult query(KpiQueryRequest request) {
        try {
            List<Map<String, Object>> results = engine.execute(request);

            // 构建详细的消息信息
            StringBuilder msgBuilder = new StringBuilder("查询成功！");
            msgBuilder.append(" 返回 ").append(results.size()).append(" 条记录");
            if (request.includeHistoricalData()) {
                msgBuilder.append(" [包含历史数据]");
            }
            if (request.includeTargetData()) {
                msgBuilder.append(" [包含目标值]");
            }

            // 使用与v1完全一致的返回格式：dataArray, status, msg
            return new KpiQueryResult(results, "0000", msgBuilder.toString());

        } catch (Exception e) {
            // 错误格式也与v1一致
            return new KpiQueryResult(List.of(), "9999", "查询失败: " + e.getMessage());
        }
    }

}
