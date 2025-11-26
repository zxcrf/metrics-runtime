package com.asiainfo.metrics.etl;

import com.asiainfo.metrics.service.MinIOService;
import io.quarkus.test.junit.QuarkusTest;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.List;

@QuarkusTest
public class ProducerDemoTest {

    @jakarta.inject.Inject
    MinIOService minIOService;

    @Test
//    @Disabled("这是一个手动执行的 Demo，需要真实的 MinIO 和 MySQL 环境")
    public void testProduceRealData() throws Exception {
        // 1. 配置连接信息
        String minioUrl = "http://10.19.28.145:19090";
        String mysqlUrl = "jdbc:mysql://localhost:3306/kpi_2025";

        SqliteDataProducer producer = new SqliteDataProducer(
                minioUrl, "modo","Modo@123","metrics-runtime"
        );

        try (Connection mysqlConn = DriverManager.getConnection(mysqlUrl, "root", "root")) {

            // 2. 生产 KD1001 (CD001: 只有 city_id)
            // 源 SQL: 必须按照你的源表结构来写
            String kpiSql = """
                SELECT op_time, kpi_id,city_id, sum(kpi_val) as kpi_val
                FROM kpi_day_CD003 
                WHERE op_time = '20251024'
                group by op_time, kpi_id,city_id
            """;

            producer.produceKpiData(
                    mysqlConn,
                    kpiSql,
                    "KD1002",
                    "20251024",
                    "CD001",
                    List.of("city_id")
            );

            // 3. 生产维度表 CD001
            String dimSql = """
                select dim_code, dim_val, dim_id, parent_dim_code
                        from kpi_dim_CD003
            """;

            producer.produceDimData(
                    mysqlConn,
                    dimSql,
                    "CD001",
                    List.of("city_id")
            );
        }
    }
}