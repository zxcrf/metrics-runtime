package com.asiainfo.metrics.service;

import com.asiainfo.metrics.model.KpiQueryRequest;
import com.asiainfo.metrics.model.KpiQueryResult;

import java.util.concurrent.CompletableFuture;

/**
 *
 *
 * @author QvQ
 * @date 2025/11/11
 */
public interface KpiQueryEngine {
    CompletableFuture<KpiQueryResult> queryKpiDataAsync(KpiQueryRequest request);
    KpiQueryResult queryKpiData(KpiQueryRequest request);
}
