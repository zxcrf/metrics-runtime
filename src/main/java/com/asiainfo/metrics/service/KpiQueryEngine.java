package com.asiainfo.metrics.service;

import com.asiainfo.metrics.model.http.KpiQueryRequest;
import com.asiainfo.metrics.model.http.KpiQueryResult;

/**
 * KPI查询引擎统一接口
 * 所有引擎实现必须遵循以下约定：
 * 1. 查询结果必须按「维度组合+opTime」聚合
 * 2. 聚合结果包含opTime和kpiValues字段
 * 3. 缺失值使用NOT_EXISTS("--")占位符
 *
 * @author QvQ
 * @date 2025/11/11
 */
public interface KpiQueryEngine {
    static final String NOT_EXISTS = "--";
//
//    /**
//     * 异步查询KPI数据
//     * @param request KPI查询请求
//     * @return 包含聚合结果的异步未来
//     */
//    CompletableFuture<KpiQueryResult> queryKpiDataAsync(KpiQueryRequest request);

    /**
     * 同步查询KPI数据
     * @param request KPI查询请求
     * @return 包含聚合结果的响应
     */
    KpiQueryResult queryKpiData(KpiQueryRequest request);


}
