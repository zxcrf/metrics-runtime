package com.asiainfo.metrics.service;

import com.asiainfo.metrics.repository.KpiMetadataRepository;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

@QuarkusTest
public class KpiStorageServiceTest {

    @Inject
    KpiStorageService kpiStorageService;

    @Inject
    KpiMetadataRepository kpiMetadataRepository;

    /**
     * Test that dimension fields are loaded from database, not hardcoded
     */
    @Test
    public void testGetDimFieldNamesFromDatabase() {
        // Test with CD001 (city_id)
        assertDimFields("CD001", "city_id");

        // Test with CD002 (city_id, county_id)
        assertDimFields("CD002", "city_id", "county_id");

        // Test with CD003 (city_id, county_id, region_id)
        assertDimFields("CD003", "city_id", "county_id", "region_id");
    }

    private void assertDimFields(String compDimCode, String... expectedFields) {
        java.lang.reflect.Method method;
        try {
            // Use reflection to call the private getDimFieldNames method
            method = KpiStorageService.class.getDeclaredMethod("getDimFieldNames", String.class);
            method.setAccessible(true);

            List<String> result = (List<String>) method.invoke(kpiStorageService, compDimCode);

            assertArrayEquals(expectedFields, result.toArray(),
                "Dimension fields for " + compDimCode + " do not match expected");
            System.out.println("✓ Dimension fields for " + compDimCode + ": " + String.join(", ", result));

        } catch (Exception e) {
            fail("Failed to test getDimFieldNames for " + compDimCode + ": " + e.getMessage());
        }
    }

    /**
     * Test that KpiMetadataRepository correctly loads dimensions from database
     */
    @Test
    public void testDatabaseDimensionLoading() {
        // Test that dimensions are loaded from database
        var cd001Fields = kpiMetadataRepository.getDimFieldsStringByCompDim("CD001");
        assertEquals("city_id", cd001Fields.trim(), "CD001 should have only city_id");

        var cd002Fields = kpiMetadataRepository.getDimFieldsStringByCompDim("CD002");
        assertEquals("city_id, county_id", cd002Fields.trim(), "CD002 should have city_id and county_id");
    }
}