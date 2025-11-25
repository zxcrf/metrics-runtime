package com.asiainfo.metrics.v2;

import com.asiainfo.metrics.v2.core.model.MetricDefinition;
import com.asiainfo.metrics.v2.core.model.PhysicalTableReq;
import com.asiainfo.metrics.v2.core.model.QueryContext;
import com.asiainfo.metrics.v2.core.parser.MetricParser;
import com.asiainfo.metrics.v2.infra.persistence.MetadataRepository;
import io.quarkus.test.InjectMock;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

@QuarkusTest
public class MetricParserTest {

    @Inject
    MetricParser parser;

    @InjectMock
    MetadataRepository metadataRepo;

    @Test
    public void testVirtualKpiDependencyResolution() {
        // 场景：虚拟指标 V1 = ${KD1001} + ${KD1002}
        // KD1001 -> CD001
        // KD1002 -> CD002

        String opTime = "20251104";
        QueryContext ctx = new QueryContext();
        ctx.setOpTime(opTime);

        // Mock 元数据
        Mockito.when(metadataRepo.findById("KD1001"))
                .thenReturn(MetricDefinition.physical("KD1001", "sum", "CD001"));
        Mockito.when(metadataRepo.findById("KD1002"))
                .thenReturn(MetricDefinition.physical("KD1002", "sum", "CD002"));

        // 虚拟指标定义
        MetricDefinition v1 = MetricDefinition.virtual("V1", "${KD1001} + ${KD1002}", "sum");

        // 执行解析 (注意：不再传入 compDimCode)
        parser.resolveDependencies(v1, opTime, ctx);

        Set<PhysicalTableReq> tables = ctx.getRequiredTables();
        assertEquals(2, tables.size());

        // 验证 KD1001 是否正确关联了 CD001
        assertTrue(tables.stream().anyMatch(t ->
                t.kpiId().equals("KD1001") && t.compDimCode().equals("CD001")
        ), "KD1001 应关联 CD001");

        // 验证 KD1002 是否正确关联了 CD002
        assertTrue(tables.stream().anyMatch(t ->
                t.kpiId().equals("KD1002") && t.compDimCode().equals("CD002")
        ), "KD1002 应关联 CD002");
    }

    @Test
    public void testCircularDependency() {
        // 场景：KD1001 依赖 V1，V1 依赖 KD1001
        String opTime = "20251104";
        QueryContext ctx = new QueryContext();

        // KD1001 定义为依赖 V1 (模拟配置错误)
        MetricDefinition kd1001 = MetricDefinition.composite("KD1001", "${V1}", "sum", "CD001");
        // V1 定义为依赖 KD1001
        MetricDefinition v1 = MetricDefinition.virtual("V1", "${KD1001}", "sum");

        Mockito.when(metadataRepo.findById("KD1001")).thenReturn(kd1001);
        Mockito.when(metadataRepo.findById("V1")).thenReturn(v1);

        // 断言抛出运行时异常
        assertThrows(RuntimeException.class, () -> {
            parser.resolveDependencies(kd1001, opTime, ctx);
        });
    }
}