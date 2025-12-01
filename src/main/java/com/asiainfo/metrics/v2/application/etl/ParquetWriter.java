package com.asiainfo.metrics.v2.application.etl;

import com.asiainfo.metrics.common.infrastructure.minio.KpiComputeService;
import com.asiainfo.metrics.common.infrastructure.minio.ParquetFileManager;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Parquet 写入服务 (V2)
 * 负责将计算后的指标数据写入 Parquet 文件并上传到 MinIO
 *
 * @author QvQ
 * @date 2025/11/28
 */
@ApplicationScoped
public class ParquetWriter {

    private static final Logger log = LoggerFactory.getLogger(ParquetWriter.class);

    @Inject
    ParquetFileManager parquetFileManager;

    /**
     * 写入结果
     */
    public record WriteResult(boolean success, String message, int storedCount) {

        public static WriteResult success(String message, int storedCount) {
            return new WriteResult(true, message, storedCount);
        }

        public static WriteResult error(String message) {
            return new WriteResult(false, message, 0);
        }
    }

    /**
     * 将指标数据写入 Parquet 文件并上传到 MinIO
     *
     * @param records 指标数据记录列表
     * @return 写入结果
     */
    public WriteResult writeToParquet(List<KpiComputeService.KpiDataRecord> records) {
        try {
            if (records.isEmpty()) {
                return WriteResult.success("无数据需要存储", 0);
            }

            log.info("开始写入 Parquet 文件，共 {} 条记录", records.size());

            // 按 KPI 分组 (每个 KPI 一个 Parquet 文件)
            Map<String, List<KpiComputeService.KpiDataRecord>> kpiGroup = records.stream()
                    .collect(Collectors.groupingBy(KpiComputeService.KpiDataRecord::kpiId));

            int totalStored = 0;

            // 为每个 KPI 创建单独的 Parquet 文件
            for (Map.Entry<String, List<KpiComputeService.KpiDataRecord>> entry : kpiGroup.entrySet()) {
                String kpiId = entry.getKey();
                List<KpiComputeService.KpiDataRecord> kpiRecords = entry.getValue();

                if (kpiRecords.isEmpty()) {
                    continue;
                }

                // 获取第一条记录用于提取元信息
                KpiComputeService.KpiDataRecord firstRecord = kpiRecords.get(0);
                String opTime = firstRecord.opTime();
                String compDimCode = firstRecord.compDimCode();

                log.info("处理 KPI: {}, opTime: {}, compDimCode: {}, 记录数: {}",
                        kpiId, opTime, compDimCode, kpiRecords.size());

                try {
                    // 1. 创建本地 Parquet 文件路径
                    String localPath = parquetFileManager.createParquetFilePath(kpiId, opTime, compDimCode);
                    String tableName = parquetFileManager.getParquetTableName(kpiId, opTime, compDimCode);

                    // 2. 写入 Parquet 文件
                    parquetFileManager.writeDataToParquet(localPath, tableName, kpiRecords);

                    // 3. 上传到 MinIO
                    parquetFileManager.uploadParquetFile(localPath, kpiId, opTime, compDimCode);

                    // 4. 清理本地 Parquet 文件
                    try {
                        Files.deleteIfExists(Paths.get(localPath));
                        log.debug("清理本地 Parquet 文件: {}", localPath);
                    } catch (Exception e) {
                        log.warn("清理本地 Parquet 文件失败: {}", localPath, e);
                    }

                    totalStored += kpiRecords.size();
                    log.info("KPI {} 写入成功，存储 {} 条记录", kpiId, kpiRecords.size());

                } catch (Exception e) {
                    log.error("KPI {} 写入失败", kpiId, e);
                    // 继续处理其他 KPI，不中断整个流程
                }
            }

            log.info("Parquet 写入完成，共存储 {} 条记录", totalStored);
            return WriteResult.success("Parquet 文件生成并上传成功", totalStored);

        } catch (Exception e) {
            log.error("Parquet 写入失败", e);
            return WriteResult.error("Parquet 写入失败: " + e.getMessage());
        }
    }
}
