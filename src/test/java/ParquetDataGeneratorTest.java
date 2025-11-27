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

        // 生成15个时间点的测试数据（20251001-20251015）
        String kpiId = "KD9999";
        String compDimCode = "CD002";

        for (int day = 1; day <= 15; day++) {
            String opTime = String.format("202510%02d", day);

            log.info("生成数据: kpiId={}, opTime={}", kpiId, opTime);

            // 构造100条测试数据
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
            dimValues.put("city_id", String.format("C%04d", i));
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
