package com.asiainfo.metrics.model.db;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * 组合维度定义
 * 对应metrics_comp_dim_def表
 */
public record CompDimDef(
    @JsonProperty String compDimCode,
    @JsonProperty String compDimName,
    @JsonProperty String compDimConf,  // JSON格式，包含所有原子维度
    @JsonProperty String tState,
    @JsonProperty String teamName,
    @JsonProperty String createTime,
    @JsonProperty String updateTime
) {

}
