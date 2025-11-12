package com.asiainfo.metrics.repository;

import io.agroal.api.AgroalDataSource;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

/**
 *
 *
 * @author QvQ
 * @date 2025/11/12
 */
@ApplicationScoped
public class KpiModelRepository {
    @Inject
    @io.quarkus.agroal.DataSource("metadb")
    AgroalDataSource metadbDataSource;

    
}
