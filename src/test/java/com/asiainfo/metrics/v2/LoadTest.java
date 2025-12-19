package com.asiainfo.metrics.v2;


import com.asiainfo.metrics.common.model.dto.KpiQueryRequest;
import com.asiainfo.metrics.v2.application.engine.UnifiedMetricEngine;
import com.asiainfo.metrics.v2.domain.model.MetricDefinition;
import com.asiainfo.metrics.v2.domain.model.PhysicalTableReq;
import com.asiainfo.metrics.v2.infrastructure.persistence.MetadataRepository;
import com.asiainfo.metrics.v2.infrastructure.storage.StorageManager;
import io.quarkus.test.InjectMock;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

@QuarkusTest
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class LoadTest {

    @Inject
    UnifiedMetricEngine engine;

    @InjectMock
    MetadataRepository metadataRepo;

    @InjectMock
    StorageManager storageManager;

    private final String KPI_ID = "KD9999";
    private final String COMP_DIM = "CD002";
    private final List<String> DIMS = List.of("city_id", "county_id");
    private final int DAYS = 30;
    private final int ROWS_PER_DAY = 10000; // 1万条数据/天

    private Map<String, String> kpiFileMap = new HashMap<>();
    private String dimFile;

    @BeforeAll
    public void setupData() throws Exception {
        System.out.println("Generating test data...");
        Path tempDir = Files.createTempDirectory("metrics_load_test_");
        tempDir.toFile().deleteOnExit();

        // 1. Mock Metadata
        Mockito.when(metadataRepo.findById(KPI_ID))
                .thenReturn(MetricDefinition.physical(KPI_ID, "sum", COMP_DIM));
        Mockito.when(metadataRepo.getDimCols(COMP_DIM))
                .thenReturn(new HashSet<>(DIMS));

        // 2. Generate Dimension Table (CD002)
        dimFile = tempDir.resolve("dim_cd002.db").toAbsolutePath().toString();
        try (Connection conn = DriverManager.getConnection("jdbc:sqlite:" + dimFile)) {
            Statement stmt = conn.createStatement();
            stmt.execute("CREATE TABLE kpi_dim_CD002 (dim_code TEXT, dim_val TEXT, dim_id TEXT, parent_dim_code TEXT)");
            stmt.execute("CREATE INDEX idx_dim_code ON kpi_dim_CD002(dim_code)");

            conn.setAutoCommit(false);
            PreparedStatement ps = conn.prepareStatement("INSERT INTO kpi_dim_CD002 VALUES (?, ?, ?, ?)");

            // Generate 10 cities, each with 10 counties
            for (int i = 1; i <= 10; i++) {
                String cityId = String.format("%03d", i);
                // City row
                ps.setString(1, cityId);
                ps.setString(2, "City_" + cityId);
                ps.setString(3, "city_id");
                ps.setString(4, null);
                ps.addBatch();

                for (int j = 1; j <= 10; j++) {
                    String countyId = String.format("%03d%03d", i, j);
                    // County row
                    ps.setString(1, countyId);
                    ps.setString(2, "County_" + countyId);
                    ps.setString(3, "county_id");
                    ps.setString(4, cityId);
                    ps.addBatch();
                }
            }
            ps.executeBatch();
            conn.commit();
        }

        // 3. Generate KPI Tables (30 days)
        LocalDate startDate = LocalDate.of(2025, 11, 1);
        DateTimeFormatter fmt = DateTimeFormatter.ofPattern("yyyyMMdd");
        Random random = new Random();

        for (int i = 0; i < DAYS; i++) {
            String opTime = startDate.plusDays(i).format(fmt);
            String fileName = String.format("kpi_%s_%s_%s.db", KPI_ID, opTime, COMP_DIM);
            String filePath = tempDir.resolve(fileName).toAbsolutePath().toString();
            kpiFileMap.put(opTime, filePath);

            try (Connection conn = DriverManager.getConnection("jdbc:sqlite:" + filePath)) {
                Statement stmt = conn.createStatement();
                String tableName = String.format("kpi_%s_%s_%s", KPI_ID, opTime, COMP_DIM);
                stmt.execute("CREATE TABLE " + tableName
                        + " (city_id TEXT, county_id TEXT, kpi_val REAL, kpi_id TEXT, op_time TEXT)");

                conn.setAutoCommit(false);
                PreparedStatement ps = conn.prepareStatement("INSERT INTO " + tableName + " VALUES (?, ?, ?, ?, ?)");

                for (int r = 0; r < ROWS_PER_DAY; r++) {
                    int cityNum = random.nextInt(10) + 1;
                    int countyNum = random.nextInt(10) + 1;
                    String cityId = String.format("%03d", cityNum);
                    String countyId = String.format("%03d%03d", cityNum, countyNum);

                    ps.setString(1, cityId);
                    ps.setString(2, countyId);
                    ps.setDouble(3, random.nextDouble() * 1000);
                    ps.setString(4, KPI_ID);
                    ps.setString(5, opTime);
                    ps.addBatch();
                }
                ps.executeBatch();
                conn.commit();
            }
        }

        // 4. Mock StorageManager
        Mockito.when(storageManager.downloadAndPrepare(ArgumentMatchers.any()))
                .thenAnswer(inv -> {
                    PhysicalTableReq req = inv.getArgument(0);
                    return kpiFileMap.get(req.opTime());
                });

        Mockito.when(storageManager.downloadAndCacheDimDB(ArgumentMatchers.eq(COMP_DIM)))
                .thenReturn(dimFile);

        System.out.println("Data generation complete.");
    }

    @Test
    public void testPerformance() throws InterruptedException {
        int concurrency = 10;
        int totalRequests = 100;
        ExecutorService executor = Executors.newFixedThreadPool(concurrency);
        CountDownLatch latch = new CountDownLatch(totalRequests);
        AtomicInteger successCount = new AtomicInteger(0);
        AtomicInteger failCount = new AtomicInteger(0);

        List<String> opTimes = new ArrayList<>(kpiFileMap.keySet());
        Random random = new Random();

        long start = System.currentTimeMillis();

        for (int i = 0; i < totalRequests; i++) {
            executor.submit(() -> {
                try {
                    // Randomly select 3 days
                    List<String> queryTimes = new ArrayList<>();
                    for (int d = 0; d < 3; d++) {
                        queryTimes.add(opTimes.get(random.nextInt(opTimes.size())));
                    }

                    KpiQueryRequest req = new KpiQueryRequest(
                            List.of(KPI_ID),
                            queryTimes,
                            DIMS,
                            List.of(),
                            Map.of(),
                            false,
                            false);

                    List<Map<String, Object>> result = engine.execute(req);
                    if (!result.isEmpty()) {
                        successCount.incrementAndGet();
                    } else {
                        failCount.incrementAndGet();
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    failCount.incrementAndGet();
                } finally {
                    latch.countDown();
                }
            });
        }

        latch.await();
        long end = System.currentTimeMillis();
        long duration = end - start;

        System.out.println("========================================");
        System.out.println("Load Test Results:");
        System.out.println("Total Requests: " + totalRequests);
        System.out.println("Concurrency: " + concurrency);
        System.out.println("Success: " + successCount.get());
        System.out.println("Failed: " + failCount.get());
        System.out.println("Total Time: " + duration + " ms");
        System.out.println("Avg Latency: " + (duration / (double) totalRequests) + " ms");
        System.out.println("TPS: " + (totalRequests / (duration / 1000.0)));
        System.out.println("========================================");
    }
}
