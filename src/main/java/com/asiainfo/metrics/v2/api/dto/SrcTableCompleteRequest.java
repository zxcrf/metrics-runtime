package com.asiainfo.metrics.v2.api.dto;

/**
 * srcTableComplete 请求体
 * 
 * @param srcTableName ETL完成的来源表名
 * @param opTime 批次号，如 20251219
 */
public record SrcTableCompleteRequest(
        String srcTableName,
        String opTime
) {}
