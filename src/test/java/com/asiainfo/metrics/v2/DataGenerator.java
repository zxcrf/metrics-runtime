package com.asiainfo.metrics.v2;

import io.minio.BucketExistsArgs;
import io.minio.MakeBucketArgs;
import io.minio.MinioClient;
import io.minio.PutObjectArgs;
import io.minio.RemoveObjectArgs;
import org.junit.jupiter.api.Test;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Random;
import java.util.zip.GZIPOutputStream;

public class DataGenerator {

    private static final String ENDPOINT = "http://127.0.0.1:9000";
    private static final String ACCESS_KEY = "minioadmin";
    private static final String SECRET_KEY = "minioadmin";
    private static final String BUCKET = "metrics-runtime";

    private final String KPI_ID = "KD9999";
    private final String COMP_DIM = "CD002";
    private final int DAYS = 30;
    private final int ROWS_PER_DAY = 10000;

    @Test
    public void generateAndUpload() throws Exception {
        MinioClient minioClient = MinioClient.builder()
                .endpoint(ENDPOINT)
                .credentials(ACCESS_KEY, SECRET_KEY)
                .build();

        try {
            if (!minioClient.bucketExists(BucketExistsArgs.builder().bucket(BUCKET).build())) {
                minioClient.makeBucket(MakeBucketArgs.builder().bucket(BUCKET).build());
                System.out.println("Bucket created.");
            } else {
                System.out.println("Bucket exists.");
            }
        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        }

        Path tempDir = Files.createTempDirectory("metrics_gen_");
        System.out.println("Temp dir: " + tempDir);

        // 1. Generate Dimension Table
        Path dimDbPath = tempDir.resolve("kpi_dim_" + COMP_DIM + ".db");
        try (Connection conn = DriverManager.getConnection("jdbc:sqlite:" + dimDbPath.toAbsolutePath())) {
            Statement stmt = conn.createStatement();
            stmt.execute("CREATE TABLE kpi_dim_" + COMP_DIM
                    + " (dim_code TEXT, dim_val TEXT, dim_id TEXT, parent_dim_code TEXT)");
            stmt.execute("CREATE INDEX idx_dim_code ON kpi_dim_" + COMP_DIM + "(dim_code)");

            conn.setAutoCommit(false);
            PreparedStatement ps = conn.prepareStatement("INSERT INTO kpi_dim_" + COMP_DIM + " VALUES (?, ?, ?, ?)");

            for (int i = 1; i <= 10; i++) {
                String cityId = String.format("%03d", i);
                ps.setString(1, cityId);
                ps.setString(2, "City_" + cityId);
                ps.setString(3, "city_id");
                ps.setString(4, null);
                ps.addBatch();

                for (int j = 1; j <= 10; j++) {
                    String countyId = String.format("%03d%03d", i, j);
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

        // Compress and Upload Dim
        Path dimGzPath = gzipFile(dimDbPath);
        String dimKey = "dim/kpi_dim_" + COMP_DIM + ".db.gz";
        upload(minioClient, dimGzPath, dimKey);
        System.out.println("Uploaded: " + dimKey);

        // 2. Generate KPI Tables
        LocalDate startDate = LocalDate.of(2025, 11, 1);
        DateTimeFormatter fmt = DateTimeFormatter.ofPattern("yyyyMMdd");
        Random random = new Random();

        for (int i = 0; i < DAYS; i++) {
            String opTime = startDate.plusDays(i).format(fmt);
            String fileName = String.format("kpi_%s_%s_%s.db", KPI_ID, opTime, COMP_DIM);
            Path dbPath = tempDir.resolve(fileName);

            try (Connection conn = DriverManager.getConnection("jdbc:sqlite:" + dbPath.toAbsolutePath())) {
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

            // Compress and Upload KPI
            Path gzPath = gzipFile(dbPath);
            // Key format: yyyy/yyyyMM/yyyyMMdd/CD002/filename.gz
            String year = opTime.substring(0, 4);
            String month = opTime.substring(0, 6);
            String key = String.format("%s/%s/%s/%s/%s.gz", year, month, opTime, COMP_DIM, fileName);

            upload(minioClient, gzPath, key);
            System.out.println("Uploaded: " + key);

            // Cleanup
            // Files.delete(dbPath);
            // Files.delete(gzPath);
        }

        // Files.delete(dimDbPath);
        // Files.delete(dimGzPath);
        // Files.delete(tempDir);
    }

    private Path gzipFile(Path source) throws Exception {
        Path target = source.resolveSibling(source.getFileName() + ".gz");
        try (GZIPOutputStream gos = new GZIPOutputStream(
                new BufferedOutputStream(new FileOutputStream(target.toFile())));
                FileInputStream fis = new FileInputStream(source.toFile())) {
            byte[] buffer = new byte[1024];
            int len;
            while ((len = fis.read(buffer)) > 0) {
                gos.write(buffer, 0, len);
            }
        }
        return target;
    }

    private void upload(MinioClient client, Path file, String key) throws Exception {
        int maxRetries = 3;
        for (int i = 0; i < maxRetries; i++) {
            try {
                System.out.println("Uploading " + key + " (Attempt " + (i + 1) + "/" + maxRetries + ")...");
                client.putObject(io.minio.PutObjectArgs.builder()
                        .bucket(BUCKET)
                        .object(key)
                        .stream(new java.io.FileInputStream(file.toFile()), Files.size(file), -1)
                        .contentType("application/gzip")
                        .build());
                System.out.println("Upload success: " + key);
                return; // Success
            } catch (Exception e) {
                System.err.println("Upload failed for " + key + ": " + e.getMessage());
                if (i == maxRetries - 1) {
                    throw e; // Fail after last retry
                }
                Thread.sleep(1000 * (i + 1)); // Backoff
            }
        }
    }
}
