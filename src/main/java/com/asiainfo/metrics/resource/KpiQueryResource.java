package com.asiainfo.metrics.resource;

import com.asiainfo.metrics.model.http.KpiQueryRequest;
import com.asiainfo.metrics.model.http.KpiQueryResult;
import com.asiainfo.metrics.repository.KpiMetadataRepository;
import com.asiainfo.metrics.service.KpiQueryEngine;
import com.asiainfo.metrics.service.KpiQueryEngineFactory;
import io.smallrye.common.annotation.RunOnVirtualThread;
import jakarta.inject.Inject;
import jakarta.ws.rs.*;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    KpiQueryEngineFactory engineFactory;

    @Inject
    KpiMetadataRepository metadataRepository;

    @GET
    @Path("/queryKpiDef")
    @RunOnVirtualThread
    public Response queryKpiDef(@QueryParam("kpiId") String kpiId) {
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
    @RunOnVirtualThread
    public Response queryKpiData(KpiQueryRequest request) {
        try {
            log.info("收到KPI查询请求: {} 个KPI", request.kpiArray().size());

            // 通过工厂获取当前配置的查询引擎
            KpiQueryEngine kpiQueryEngine = engineFactory.getQueryEngine();

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
     * 获取当前使用的查询引擎信息
     */
    @GET
    @Path("/engineInfo")
    public Response getEngineInfo() {
        String engineType = engineFactory.getEngineDescription();
        return Response.ok("{\"engine\": \"" + engineType + "\"}").build();
    }
}
