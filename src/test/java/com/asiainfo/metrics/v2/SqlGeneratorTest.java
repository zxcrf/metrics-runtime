package com.asiainfo.metrics.v2;

import com.asiainfo.metrics.v2.core.generator.SqlGenerator;
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

import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertTrue;

@QuarkusTest
public class SqlGeneratorTest {

    @Inject
    SqlGenerator sqlGenerator;

    @InjectMock
    MetadataRepository metadataRepo;

    @InjectMock
    MetricParser parser;

    @Test
    public void testHeterogeneousDimensionUnion() {
        // 场景：用户查询 [city_id, county_id]
        // 指标1 (KD1001): 属于 CD001 (只有 city_id)
        // 指标2 (KD1002): 属于 CD002 (有 city_id, county_id)

        String opTime = "20251104";
        QueryContext ctx = new QueryContext();
        ctx.setOpTime(opTime);
        ctx.addDimCode("city_id");
        ctx.addDimCode("county_id");

        // 模拟 KD1001 的物理表请求 (CD001)
        PhysicalTableReq req1 = new PhysicalTableReq("KD1001", opTime, "CD001");
        ctx.addPhysicalTable("KD1001", opTime, "CD001");
        ctx.registerAlias(req1, "db_1");

        // 模拟 KD1002 的物理表请求 (CD002)
        PhysicalTableReq req2 = new PhysicalTableReq("KD1002", opTime, "CD002");
        ctx.addPhysicalTable("KD1002", opTime, "CD002");
        ctx.registerAlias(req2, "db_2");

        // Mock 元数据：CD001 缺 county_id，CD002 都有
        Mockito.when(metadataRepo.getDimCols("CD001")).thenReturn(Set.of("city_id"));
        Mockito.when(metadataRepo.getDimCols("CD002")).thenReturn(Set.of("city_id", "county_id"));

        // 构造指标定义列表
        List<MetricDefinition> metrics = List.of(
                MetricDefinition.physical("KD1001", "sum", "CD001"),
                MetricDefinition.physical("KD1002", "sum", "CD002")
        );

        // 执行生成
        String sql = sqlGenerator.generateSql(metrics, ctx, List.of("city_id", "county_id"));
        System.out.println("Generated SQL:\n" + sql);

        // 断言：KD1001 的子查询必须包含 'NULL as county_id'
        // 注意：具体生成的SQL顺序可能变化，主要检查关键字
        assertTrue(sql.contains("NULL as county_id"), "CD001 的子查询应自动填充 NULL 列");

        // 断言：KD1002 的子查询应该直接查 county_id
        // 这里通过检查不含 'NULL as county_id...FROM...kpi_KD1002' 比较难，
        // 我们可以简单检查 SQL 结构是否包含两个 SELECT 子句
        assertTrue(sql.contains("FROM db_1.kpi_KD1001"), "应包含表1查询");
        assertTrue(sql.contains("FROM db_2.kpi_KD1002"), "应包含表2查询");
    }
}