package com.asiainfo.metrics.domain.model;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 *
 *
 * @author QvQ
 * @date 2025/11/12
 */
public record KpiModel(
        @JsonProperty String modelId,
        @JsonProperty String modelName,
        @JsonProperty String modelType,
        @JsonProperty String compDimCode,
        @JsonProperty String modelDsName,
        @JsonProperty String modelSql,
        @JsonProperty String state,
        @JsonProperty String teamName,
        @JsonProperty String createTime,
        @JsonProperty String updateTime
) {
}
