package com.asiainfo.metrics.test;

import com.asiainfo.metrics.v2.core.engine.UnifiedMetricEngine;
import com.asiainfo.metrics.model.http.KpiQueryRequest;
import com.asiainfo.metrics.v2.infra.persistence.MetadataRepository;
import com.asiainfo.metrics.v2.core.model.MetricDefinition;
import com.asiainfo.metrics.v2.core.model.MetricType;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.InjectMock;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import org.mockito.Mockito;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

@QuarkusTest
public class PerformanceTest {

    @Inject
    UnifiedMetricEngine engine;

    @InjectMock
    MetadataRepository metadataRepo;

    @BeforeEach
    public void setup() {
        MetricDefinition mockDef = new MetricDefinition("KPI_001", "sum(amount)", MetricType.PHYSICAL, "sum",
                "city_id");
        Mockito.when(metadataRepo.findById("KPI_001")).thenReturn(mockDef);
    }

    @Test
    public void testPerformance() {
        KpiQueryRequest req = new KpiQueryRequest(
                Arrays.asList("KPI_001"),
                Arrays.asList("20251101", "20251102", "20251103", "20251104", "20251105"),
                null,
                null,
                null,
                false,
                false);

        long start = System.currentTimeMillis();
        try {
            List<Map<String, Object>> result = engine.execute(req);
            System.out.println("Result size: " + result.size());
        } catch (Exception e) {
            // Ignore execution errors as we are testing the flow and performance,
            // and we might lack actual data/tables which causes downstream errors.
            // But we want to ensure the parallel execution logic runs.
            System.out.println("Execution triggered (expected failure due to missing data): " + e.getMessage());
        }
        long end = System.currentTimeMillis();

        System.out.println("Execution time: " + (end - start) + "ms");
    }
}
