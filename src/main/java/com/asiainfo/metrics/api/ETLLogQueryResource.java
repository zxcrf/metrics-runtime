package com.asiainfo.metrics.api;

import com.asiainfo.metrics.application.etl.ETLExecutionLogger;
import com.asiainfo.metrics.application.etl.ETLLogQueryService;
import jakarta.inject.Inject;
import jakarta.ws.rs.*;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ETL执行日志查询API
 * 提供日志查询、统计和历史记录接口
 */
@Path("/api/v2/etl/logs")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class ETLLogQueryResource {

    private static final Logger log = LoggerFactory.getLogger(ETLLogQueryResource.class);

    @Inject
    ETLLogQueryService queryService;

    /**
     * 分页查询ETL执行日志
     */
    @GET
    public Response queryLogs(
            @QueryParam("kpiModelId") String kpiModelId,
            @QueryParam("opTime") String opTime,
            @QueryParam("status") String status,
            @QueryParam("executionType") String executionType,
            @QueryParam("startDate") String startDate,
            @QueryParam("endDate") String endDate,
            @QueryParam("page") @DefaultValue("1") int page,
            @QueryParam("pageSize") @DefaultValue("20") int pageSize) {

        try {
            var result = queryService.queryLogs(
                    kpiModelId, opTime, status, executionType,
                    startDate, endDate, page, pageSize);
            return Response.ok(result).build();
        } catch (Exception e) {
            log.error("[ETL Log Query] Failed to query logs", e);
            return Response.status(500).entity("Query failed: " + e.getMessage()).build();
        }
    }

    /**
     * 获取单条日志详情
     */
    @GET
    @Path("/{id}")
    public Response getLogDetail(@PathParam("id") Long id) {
        try {
            var detail = queryService.getLogDetail(id);
            if (detail == null) {
                return Response.status(404).entity("Log not found").build();
            }
            return Response.ok(detail).build();
        } catch (Exception e) {
            log.error("[ETL Log Query] Failed to get log detail: {}", id, e);
            return Response.status(500).entity("Query failed: " + e.getMessage()).build();
        }
    }

    /**
     * 获取模型的最近执行记录
     */
    @GET
    @Path("/latest/{kpiModelId}")
    public Response getLatestExecutions(
            @PathParam("kpiModelId") String kpiModelId,
            @QueryParam("limit") @DefaultValue("10") int limit) {

        try {
            var recent = queryService.getRecentExecutions(kpiModelId, limit);
            return Response.ok(recent).build();
        } catch (Exception e) {
            log.error("[ETL Log Query] Failed to get recent executions: {}", kpiModelId, e);
            return Response.status(500).entity("Query failed: " + e.getMessage()).build();
        }
    }

    /**
     * 获取统计信息
     */
    @GET
    @Path("/stats")
    public Response getStats(
            @QueryParam("startDate") String startDate,
            @QueryParam("endDate") String endDate) {

        try {
            var stats = queryService.getStats(startDate, endDate);
            return Response.ok(stats).build();
        } catch (Exception e) {
            log.error("[ETL Log Query] Failed to get stats", e);
            return Response.status(500).entity("Query failed: " + e.getMessage()).build();
        }
    }
}
