package com.asiainfo.metrics.resource;

import com.asiainfo.metrics.model.KpiQueryRequest;
import com.asiainfo.metrics.model.KpiQueryResult;
import com.asiainfo.metrics.repository.KpiMetadataRepository;
import com.asiainfo.metrics.service.KpiQueryEngine;
import jakarta.inject.Inject;
import jakarta.ws.rs.*;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletionStage;

/**
 * KPI查询REST API
 * 提供高性能的KPI数据查询接口
 */
@Path("/api/kpi")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class KpiQueryResource {

    private static final Logger log = LoggerFactory.getLogger(KpiQueryResource.class);

    @Inject
    KpiQueryEngine kpiQueryEngine;

    @Inject
    KpiMetadataRepository metadataRepository;

    @GET
    @Path("/queryKpiDef")
    public Response queryKpiDef(@QueryParam("kpiId") String kpiId) {
        // 直接返回对象，JAX-RS会自动序列化为JSON（类似SpringBoot）
        return Response.ok(metadataRepository.getKpiDefinition(kpiId)).build();
    }


    /**
     * 同步查询KPI数据
     * 返回纵表格式
     *
     * @param request 查询请求
     * @return KPI数据
     */
    @POST
    @Path("/queryKpiData")
    public Response queryKpiData(KpiQueryRequest request) {
        try {
            log.info("收到KPI查询请求: {} 个KPI", request.kpiArray().size());

            KpiQueryResult result = kpiQueryEngine.queryKpiData(request);

            log.info("查询完成: {} 条记录", result.dataArray().size());
            return Response.ok(result).build();

        } catch (Exception e) {
            log.error("查询KPI数据失败", e);
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                    .entity("{\"error\": \"" + e.getMessage() + "\"}")
                    .build();
        }
    }

    /**
     * 异步查询KPI数据
     * 使用虚拟线程处理，适用于长时间运行的查询
     *
     * @param request 查询请求
     * 异步查询任务ID
     */
    @POST
    @Path("/queryKpiDataAsync")
    public CompletionStage<Response> queryKpiDataAsync(KpiQueryRequest request) {
        log.info("收到KPI异步查询请求: {} 个KPI", request.kpiArray().size());

        return kpiQueryEngine.queryKpiDataAsync(request)
                .thenApply(result -> {
                    log.info("异步查询完成: {} 条记录", result.dataArray().size());
                    return Response.ok(result).build();
                })
                .exceptionally(throwable -> {
                    log.error("异步查询失败", throwable);
                    return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                            .entity("{\"error\": \"" + throwable.getMessage() + "\"}")
                            .build();
                });
    }
}
