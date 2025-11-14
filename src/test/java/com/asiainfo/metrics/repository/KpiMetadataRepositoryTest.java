package com.asiainfo.metrics.repository;

import com.asiainfo.metrics.model.db.CompDimDef;
import com.asiainfo.metrics.model.db.DimDef;
import io.quarkus.test.junit.QuarkusTest;
import org.junit.jupiter.api.Test;

import jakarta.inject.Inject;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

@QuarkusTest
public class KpiMetadataRepositoryTest {

    @Inject
    KpiMetadataRepository kpiMetadataRepository;

    /**
     * Test that comp dim definitions are loaded from metrics_comp_dim_def table
     */
    @Test
    public void testGetCompDimDef() {
        CompDimDef compDimDef = kpiMetadataRepository.getCompDimDef("CD001");
        assertNotNull(compDimDef, "CD001 should exist in metrics_comp_dim_def");
        assertEquals("CD001", compDimDef.compDimCode());
        assertNotNull(compDimDef.compDimConf(), "CD001 should have compDimConf");
        System.out.println("✓ Loaded comp dim definition for CD001: " + compDimDef.compDimName());
    }

    /**
     * Test that dim definitions are loaded from metrics_dim_def table
     */
    @Test
    public void testGetDimDefsByCompDim() {
        List<DimDef> dimDefs = kpiMetadataRepository.getDimDefsByCompDim("CD003");
        assertNotNull(dimDefs);
        assertFalse(dimDefs.isEmpty(), "CD003 should have dimension definitions");
        System.out.println("✓ Loaded " + dimDefs.size() + " dimension(s) for CD003:");
        dimDefs.forEach(dimDef -> {
            assertNotNull(dimDef.dbColName());
            System.out.println("  - " + dimDef.dimName() + " (" + dimDef.dimCode() + ") → " + dimDef.dbColName());
        });
        assertEquals(3, dimDefs.size(), "CD003 should have 3 dimensions");
    }

    /**
     * Test that dim fields string is correctly generated
     */
    @Test
    public void testGetDimFieldsStringByCompDim() {
        // Test with valid comp dim codes
        String cd001Fields = kpiMetadataRepository.getDimFieldsStringByCompDim("CD001");
        assertEquals("city_id", cd001Fields, "CD001 should return city_id");

        String cd002Fields = kpiMetadataRepository.getDimFieldsStringByCompDim("CD002");
        assertEquals("city_id, county_id", cd002Fields, "CD002 should return city_id, county_id");

        String cd003Fields = kpiMetadataRepository.getDimFieldsStringByCompDim("CD003");
        assertEquals("city_id, county_id, region_id", cd003Fields, "CD003 should return city_id, county_id, region_id");

        // Test with invalid comp dim code (should return default)
//        String invalidFields = kpiMetadataRepository.getDimFieldsStringByCompDim("INVALID_CODE");
//        assertEquals("city_id, county_id, region_id", invalidFields, "Invalid comp dim should return default fields");
        assertThrows(RuntimeException.class, () -> kpiMetadataRepository.getDimFieldsStringByCompDim("INVALID_CODE"));
        System.out.println("✓ All dimension field string tests passed");
    }
}