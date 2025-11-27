package com.asiainfo.metrics.tool;

import com.asiainfo.metrics.service.ParquetFileManager;
import com.asiainfo.metrics.service.KpiComputeService;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Parquet测试数据生成工具
 * 直接生成Parquet文件并上传到MinIO，跳过SQLite阶段
 */
@QuarkusTest
public class ParquetDataGeneratorTest {

    private static final Logger log = LoggerFactory.getLogger(ParquetDataGeneratorTest.class);

    @Inject
    ParquetFileManager parquetFileManager;

    @Test
    public void generateTestData() throws Exception {
        log.info("开始生成Parquet测试数据...");

        // 定义常量
        String kpiId = "KD9999";
        String compDimCode = "CD002";

        // ==========================================
        // 1. 生成维度表数据 (kpi_dim_CD002)
        // ==========================================
        log.info("开始生成维度表数据: {}", compDimCode);
        List<Map<String, String>> dimRecords = new ArrayList<>();

        // 模拟生成 100 个城市维度 (C0001 - C0100) 和对应的区县
        for (int i = 1; i <= 100; i++) {
            String cityCode = String.format("C%04d", i);

            // 插入 City 行
            Map<String, String> cityRow = new HashMap<>();
            cityRow.put("dim_code", cityCode);
            cityRow.put("dim_val", "City_" + i);
            cityRow.put("dim_id", "city_id");
            cityRow.put("parent_dim_code", null);
            dimRecords.add(cityRow);

            // 插入 County 行 (补充 county 数据，对应 KPI 数据)
            Map<String, String> countyRow = new HashMap<>();
            String countyCode = "K" + cityCode; // 简单模拟：K + C0001
            countyRow.put("dim_code", countyCode);
            countyRow.put("dim_val", "County_Of_" + i);
            countyRow.put("dim_id", "county_id");
            countyRow.put("parent_dim_code", cityCode); // 逻辑归属
            dimRecords.add(countyRow);
        }

        // 创建维度表 Parquet 文件路径
        String dimLocalPath = parquetFileManager.createDimParquetFilePath(compDimCode);
        String dimTableName = "kpi_dim_" + compDimCode;

        // 写入维度数据
        parquetFileManager.writeDimDataToParquet(dimLocalPath, dimTableName, dimRecords);

        // 上传维度表
        parquetFileManager.uploadDimParquetFile(dimLocalPath, compDimCode);

        // 清理本地维度文件
        java.nio.file.Files.deleteIfExists(java.nio.file.Paths.get(dimLocalPath));
        log.info("维度表生成完成: {}", compDimCode);


        // ==========================================
        // 2. 生成 KPI 数据 (KD9999)
        // ==========================================
        // 生成15个时间点的测试数据（20251101-20251115）
        // 注意：你的报错中涉及 20251128，建议这里生成整个11月的数据
        for (int day = 1; day <= 30; day++) {
            String opTime = String.format("202511%02d", day);

            log.info("生成KPI数据: kpiId={}, opTime={}", kpiId, opTime);

            // 构造100条测试数据，对应上面的100个城市
            List<KpiComputeService.KpiDataRecord> records = generateTestRecords(kpiId, opTime, compDimCode, 100);

            // 创建Parquet文件
            String localPath = parquetFileManager.createParquetFilePath(kpiId, opTime, compDimCode);
            String tableName = parquetFileManager.getParquetTableName(kpiId, opTime, compDimCode);

            // 写入Parquet
            parquetFileManager.writeDataToParquet(localPath, tableName, records);

            // 上传到MinIO
            parquetFileManager.uploadParquetFile(localPath, kpiId, opTime, compDimCode);

            // 清理本地文件
            java.nio.file.Files.deleteIfExists(java.nio.file.Paths.get(localPath));

            log.info("完成: opTime={}", opTime);
        }

        log.info("所有Parquet测试数据生成完成！");
    }

    /**
     * 生成测试数据记录
     */
    private List<KpiComputeService.KpiDataRecord> generateTestRecords(
            String kpiId, String opTime, String compDimCode, int count) {
        List<KpiComputeService.KpiDataRecord> records = new ArrayList<>();

        for (int i = 1; i <= count; i++) {
            Map<String, Object> dimValues = new HashMap<>();

            String cityCode = String.format("C%04d", i);
            String countyCode = "K" + cityCode;

            // 【关键修复】同时生成 city_id 和 county_id
            // DuckDB 只有在数据文件中存在该列时才能查询
            dimValues.put("city_id", cityCode);
            dimValues.put("county_id", countyCode);

            dimValues.put("op_time", opTime);

            double kpiVal = 100.0 + Math.random() * 900.0; // 100-1000之间的随机值

            KpiComputeService.KpiDataRecord record = new KpiComputeService.KpiDataRecord(
                    kpiId,
                    opTime,
                    compDimCode,
                    dimValues,
                    kpiVal);

            records.add(record);
        }

        return records;
    }
}