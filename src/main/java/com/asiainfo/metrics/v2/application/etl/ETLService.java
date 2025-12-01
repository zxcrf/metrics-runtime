package com.asiainfo.metrics.v2.application.etl;

import com.asiainfo.metrics.common.infrastructure.minio.KpiComputeService;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * ETL 服务 (V2)
 * 负责数据抽取、计算和存储的协调
 *
 * @author QvQ
 * @date 2025/11/28
 */
@ApplicationScoped
public class ETLService {

    private static final Logger log = LoggerFactory.getLogger(ETLService.class);

    @Inject
    KpiComputeService kpiComputeService;

    @Inject
    ParquetWriter parquetWriter;

    /**
     * ETL 结果
     */
    public record ETLResult(
            boolean success,
            String message,
            int computeCount,
            int storedCount) {

        public static ETLResult success(String message, int computeCount, int storedCount) {
            return new ETLResult(true, message, computeCount, storedCount);
        }

        public static ETLResult error(String message) {
            return new ETLResult(false, message, 0, 0);
        }
    }

    /**
     * 处理 ETL 流程
     * 1. 从源表抽取数据并计算派生指标
     * 2. 将结果写入 Parquet 文件
     * 3. 上传到 MinIO
     *
     * @param tableName 源表名称
     * @param opTime    批次时间
     * @return ETL 结果
     */
    public ETLResult processETL(String tableName, String opTime) {
        log.info("开始处理 ETL，源表：{}，批次：{}", tableName, opTime);

        try {
            // 1. 计算派生指标
            KpiComputeService.ComputeResult computeResult = kpiComputeService.computeExtendedMetrics(
                    tableName, opTime);

            if (!computeResult.success()) {
                log.error("指标计算失败：{}", computeResult.message());
                return ETLResult.error("指标计算失败: " + computeResult.message());
            }

            List<KpiComputeService.KpiDataRecord> data = computeResult.data();
            log.info("指标计算成功，生成 {} 条数据", data.size());

            // 2. 写入 Parquet 并上传到 MinIO
            ParquetWriter.WriteResult writeResult = parquetWriter.writeToParquet(data);

            if (!writeResult.success()) {
                log.error("Parquet 写入失败：{}", writeResult.message());
                return ETLResult.error("Parquet 写入失败: " + writeResult.message());
            }

            log.info("Parquet 写入成功，存储 {} 条记录", writeResult.storedCount());

            // 3. 返回成功结果
            return ETLResult.success(
                    "ETL 处理成功",
                    data.size(),
                    writeResult.storedCount());

        } catch (Exception e) {
            log.error("ETL 处理过程发生异常", e);
            return ETLResult.error("ETL 处理失败: " + e.getMessage());
        }
    }
}
