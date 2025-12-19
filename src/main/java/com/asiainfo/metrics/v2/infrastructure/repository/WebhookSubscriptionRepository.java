package com.asiainfo.metrics.v2.infrastructure.repository;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.inject.Named;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.*;
import java.util.*;

/**
 * Webhook订阅 Repository
 * 管理 metrics_webhook_subscription 表
 */
@ApplicationScoped
public class WebhookSubscriptionRepository {

    private static final Logger log = LoggerFactory.getLogger(WebhookSubscriptionRepository.class);

    @Inject
    @Named("metadb")
    DataSource dataSource;

    /**
     * Webhook订阅记录
     */
    public record WebhookSubscription(
            Long id,
            String kpiId,
            String callbackUrl,
            String secret,
            int retryNum,
            String status,
            String teamName,
            String userId
    ) {}

    /**
     * 根据指标ID列表查找启用的订阅
     * 
     * @param kpiIds 指标ID集合
     * @return 启用的订阅列表
     */
    public List<WebhookSubscription> findByKpiIdIn(Collection<String> kpiIds) {
        if (kpiIds == null || kpiIds.isEmpty()) {
            return Collections.emptyList();
        }

        String placeholders = String.join(",", Collections.nCopies(kpiIds.size(), "?"));
        String sql = String.format("""
            SELECT * FROM metrics_webhook_subscription 
            WHERE kpi_id IN (%s) AND status = '1'
            """, placeholders);

        List<WebhookSubscription> subscriptions = new ArrayList<>();

        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {

            int idx = 1;
            for (String kpiId : kpiIds) {
                stmt.setString(idx++, kpiId);
            }

            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    subscriptions.add(mapRow(rs));
                }
            }

            log.debug("[Webhook] Found {} subscriptions for {} kpis", 
                    subscriptions.size(), kpiIds.size());

        } catch (SQLException e) {
            log.error("[Webhook] Failed to find subscriptions by kpiIds", e);
            throw new RuntimeException("Failed to find subscriptions", e);
        }

        return subscriptions;
    }

    private WebhookSubscription mapRow(ResultSet rs) throws SQLException {
        return new WebhookSubscription(
                rs.getLong("id"),
                rs.getString("kpi_id"),
                rs.getString("callback_url"),
                rs.getString("secret"),
                rs.getInt("retry_num"),
                rs.getString("status"),
                rs.getString("team_name"),
                rs.getString("user_id")
        );
    }
}
