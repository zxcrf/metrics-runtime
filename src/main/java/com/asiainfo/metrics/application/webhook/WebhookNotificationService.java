package com.asiainfo.metrics.application.webhook;

import com.asiainfo.metrics.infrastructure.persistence.WebhookSubscriptionRepository;
import com.asiainfo.metrics.infrastructure.persistence.WebhookSubscriptionRepository.WebhookSubscription;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

/**
 * Webhook 通知服务
 * 负责在指标更新后通知订阅者
 */
@ApplicationScoped
public class WebhookNotificationService {

    private static final Logger log = LoggerFactory.getLogger(WebhookNotificationService.class);

    @Inject
    WebhookSubscriptionRepository subscriptionRepository;

    @Inject
    ObjectMapper objectMapper;

    private final HttpClient httpClient = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(10))
            .build();

    private final ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();

    /**
     * 订阅分组键（按 callbackUrl + retryNum + secret 聚合）
     */
    private record SubscriptionGroup(String callbackUrl, int retryNum, String secret) {}

    /**
     * 通知指标更新
     * 
     * @param modelId 模型ID
     * @param opTime 批次号
     * @param extendedKpiIds 本次抽取的派生指标ID列表
     */
    public void notifyKpiUpdated(String modelId, String opTime, List<String> extendedKpiIds) {
        if (extendedKpiIds == null || extendedKpiIds.isEmpty()) {
            log.debug("[Webhook] No extended KPIs to notify");
            return;
        }

        try {
            // 1. 获取影响范围（目前仅使用派生指标，后续可扩展递归查找复合指标）
            Set<String> affectedKpiIds = new HashSet<>(extendedKpiIds);
            // TODO: 追踪影响范围，递归查找依赖这些指标的复合指标
            // affectedKpiIds.addAll(findComputedKpisDependingOn(extendedKpiIds));

            // 2. 查询订阅
            List<WebhookSubscription> subscriptions = subscriptionRepository.findByKpiIdIn(affectedKpiIds);

            if (subscriptions.isEmpty()) {
                log.debug("[Webhook] No subscriptions found for KPIs: {}", affectedKpiIds);
                return;
            }

            log.info("[Webhook] Found {} subscriptions for {} KPIs", 
                    subscriptions.size(), affectedKpiIds.size());

            // 3. 按配置聚合
            Map<SubscriptionGroup, List<WebhookSubscription>> grouped = subscriptions.stream()
                    .collect(Collectors.groupingBy(s -> 
                            new SubscriptionGroup(s.callbackUrl(), s.retryNum(), s.secret())));

            // 4. 异步发送通知
            for (var entry : grouped.entrySet()) {
                SubscriptionGroup group = entry.getKey();
                Set<String> kpiIdsForGroup = entry.getValue().stream()
                        .map(WebhookSubscription::kpiId)
                        .collect(Collectors.toSet());
                        
                asyncNotifyWithRetry(group, modelId, opTime, kpiIdsForGroup, affectedKpiIds);
            }

        } catch (Exception e) {
            log.error("[Webhook] Failed to process notifications: model={}, opTime={}", 
                    modelId, opTime, e);
        }
    }

    /**
     * 异步发送通知（带指数回退重试）
     */
    private void asyncNotifyWithRetry(SubscriptionGroup group, String modelId, String opTime, 
                                       Set<String> subscribedKpiIds, Set<String> allAffectedKpiIds) {
        CompletableFuture.runAsync(() -> {
            int maxRetries = group.retryNum();
            
            for (int attempt = 0; attempt <= maxRetries; attempt++) {
                try {
                    if (attempt > 0) {
                        // 指数回退：1s, 2s, 4s, ...
                        long delay = (long) Math.pow(2, attempt - 1) * 1000;
                        log.info("[Webhook] Retry {} after {}ms: {}", attempt, delay, group.callbackUrl());
                        Thread.sleep(delay);
                    }

                    sendNotification(group, modelId, opTime, allAffectedKpiIds);
                    log.info("[Webhook] Notification sent successfully: {}", group.callbackUrl());
                    return; // 成功后退出

                } catch (Exception e) {
                    log.warn("[Webhook] Notification failed (attempt {}): {}", 
                            attempt + 1, e.getMessage());
                    
                    if (attempt == maxRetries) {
                        log.error("[Webhook] All retries exhausted for: {}", group.callbackUrl());
                    }
                }
            }
        }, executor);
    }

    /**
     * 发送通知
     */
    private void sendNotification(SubscriptionGroup group, String modelId, String opTime, 
                                  Set<String> affectedKpiIds) throws Exception {
        // 构建通知内容
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("event", "KPI_UPDATED");
        payload.put("modelId", modelId);
        payload.put("opTime", opTime);
        payload.put("affectedKpiIds", new ArrayList<>(affectedKpiIds));
        payload.put("timestamp", OffsetDateTime.now().format(DateTimeFormatter.ISO_OFFSET_DATE_TIME));

        String body = objectMapper.writeValueAsString(payload);
        String url = group.callbackUrl();

        HttpRequest.Builder requestBuilder = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .header("Content-Type", "application/json")
                .timeout(Duration.ofSeconds(30))
                .POST(HttpRequest.BodyPublishers.ofString(body));

        // 应用 Secret 鉴权
        ApplySecretResult secretResult = applySecret(requestBuilder, body, group.secret());
        if (secretResult.modifiedBody() != null) {
            body = secretResult.modifiedBody();
            requestBuilder.POST(HttpRequest.BodyPublishers.ofString(body));
        }
        if (secretResult.modifiedUrl() != null) {
            requestBuilder.uri(URI.create(secretResult.modifiedUrl()));
        }

        HttpRequest request = requestBuilder.build();
        HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

        if (response.statusCode() >= 400) {
            throw new RuntimeException("HTTP " + response.statusCode() + ": " + response.body());
        }
    }

    /**
     * Secret 应用结果
     */
    private record ApplySecretResult(String modifiedUrl, String modifiedBody) {}

    /**
     * 解析并应用 Secret 三元组
     * 格式：类型,键,值
     * - Query,token,abc123 → URL追加 ?token=abc123
     * - Header,Authorization,Bearer xxx → 添加请求头
     * - Body,json,{"appId":"xxx"} → 合并到请求体
     */
    private ApplySecretResult applySecret(HttpRequest.Builder builder, String body, String secret) {
        if (secret == null || secret.isBlank()) {
            return new ApplySecretResult(null, null);
        }

        String[] parts = secret.split(",", 3);
        if (parts.length < 3) {
            log.warn("[Webhook] Invalid secret format: {}", secret);
            return new ApplySecretResult(null, null);
        }

        String type = parts[0].trim();
        String key = parts[1].trim();
        String value = parts[2].trim();

        try {
            switch (type.toLowerCase()) {
                case "query" -> {
                    // 无法直接修改URL，返回修改后的URL
                    // 由调用方处理
                    return new ApplySecretResult(null, null); // 暂不支持，需要URL修改
                }
                case "header" -> {
                    builder.header(key, value);
                }
                case "body" -> {
                    // 合并JSON
                    if ("json".equalsIgnoreCase(key)) {
                        Map<String, Object> original = objectMapper.readValue(body, 
                                new com.fasterxml.jackson.core.type.TypeReference<Map<String, Object>>() {});
                        Map<String, Object> additional = objectMapper.readValue(value, 
                                new com.fasterxml.jackson.core.type.TypeReference<Map<String, Object>>() {});
                        original.putAll(additional);
                        return new ApplySecretResult(null, objectMapper.writeValueAsString(original));
                    }
                }
                default -> log.warn("[Webhook] Unknown secret type: {}", type);
            }
        } catch (Exception e) {
            log.error("[Webhook] Failed to apply secret: {}", e.getMessage());
        }

        return new ApplySecretResult(null, null);
    }
}
