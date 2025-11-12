package com.asiainfo.metrics.model;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * 原子维度定义
 * 对应metrics_dim_def表
 */
public record DimDef(
    @JsonProperty String dimCode,
    @JsonProperty String dimName,
    @JsonProperty String dimType,
    @JsonProperty String dimValType,
    @JsonProperty String dimValConf,
    @JsonProperty String dimDesc,
    @JsonProperty String dbColName,  // 对应的数据库列名，如city_id
    @JsonProperty String tState,
    @JsonProperty String createTime,
    @JsonProperty String updateTime
) {}
