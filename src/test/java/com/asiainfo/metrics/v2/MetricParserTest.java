package com.asiainfo.metrics.v2;

import com.asiainfo.metrics.v2.domain.model.MetricDefinition;
import com.asiainfo.metrics.v2.domain.model.PhysicalTableReq;
import com.asiainfo.metrics.v2.domain.model.QueryContext;
import com.asiainfo.metrics.v2.domain.parser.MetricParser;
import com.asiainfo.metrics.v2.infrastructure.persistence.MetadataRepository;
import io.quarkus.test.InjectMock;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

@QuarkusTest
public class MetricParserTest {

    @Inject
    MetricParser parser;

    @InjectMock
    MetadataRepository metadataRepo;

    @Test
    public void testVirtualKpiDependencyResolution() {
        // ... (这个测试保持不变，它是通过的) ...
        String opTime = "20251104";
        QueryContext ctx = new QueryContext();
        ctx.setOpTime(opTime);

        Mockito.when(metadataRepo.findById("KD1001"))
                .thenReturn(MetricDefinition.physical("KD1001", "sum", "CD001"));
        Mockito.when(metadataRepo.findById("KD1002"))
                .thenReturn(MetricDefinition.physical("KD1002", "sum", "CD002"));

        MetricDefinition v1 = MetricDefinition.virtual("V1", "${KD1001} + ${KD1002}", "sum");

        parser.resolveDependencies(v1, opTime, ctx);

        Set<PhysicalTableReq> tables = ctx.getRequiredTables();
        assertEquals(2, tables.size());
    }

    @Test
    public void testCircularDependency() {
        String opTime = "20251104";
        QueryContext ctx = new QueryContext();

        // 【关键修复】：使用符合正则 (K[DCYM]\d{4}) 的 ID
        // 场景：KD1001 依赖 KD1002，KD1002 依赖 KD1001

        // 1. 定义 KD1001 依赖 ${KD1002}
        MetricDefinition kd1001 = MetricDefinition.composite("KD1001", "${KD1002}", "sum", "CD001");

        // 2. 定义 KD1002 依赖 ${KD1001}
        MetricDefinition kd1002 = MetricDefinition.composite("KD1002", "${KD1001}", "sum", "CD001");

        // 3. Mock 行为
        Mockito.when(metadataRepo.findById("KD1001")).thenReturn(kd1001);
        Mockito.when(metadataRepo.findById("KD1002")).thenReturn(kd1002);

        // 4. 断言异常
        assertThrows(RuntimeException.class, () -> {
            parser.resolveDependencies(kd1001, opTime, ctx);
        });
    }
}