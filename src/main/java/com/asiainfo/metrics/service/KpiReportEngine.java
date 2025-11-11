package com.asiainfo.metrics.service;

import com.asiainfo.metrics.model.KpiQueryPageResult;
import com.asiainfo.metrics.model.KpiQueryRequest;

import java.util.concurrent.CompletableFuture;

/**
 *
 *
 * @author QvQ
 * @date 2025/11/11
 */
public interface KpiReportEngine {
    CompletableFuture<KpiQueryPageResult> queryReportDataAsync(KpiQueryRequest request);
    KpiQueryPageResult queryReportData(KpiQueryRequest request);
}
