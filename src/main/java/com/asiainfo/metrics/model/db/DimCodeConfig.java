package com.asiainfo.metrics.model.db;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * 维度编码配置
 * 用于解析comp_dim_conf JSON字段
 */
public record DimCodeConfig(
    @JsonProperty String dimCode,
    @JsonProperty String parentDimCode
) {}
