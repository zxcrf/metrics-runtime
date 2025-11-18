package com.asiainfo.metrics.service;

import com.asiainfo.metrics.model.db.DimDef;
import com.asiainfo.metrics.model.db.KpiDefinition;
import com.asiainfo.metrics.model.http.KpiQueryRequest;
import io.agroal.api.AgroalDataSource;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;
import java.util.stream.Collectors;

/**
 * KPI计算引擎
 * 使用SQLite内存计算进行嵌套指标计算
 *
 * 核心特性:
 * - 分层计算: 通过临时表支持嵌套依赖
 * - 虚拟线程: 每个请求一个虚拟线程
 * - 内存优化: 使用内存SQLite数据库
 * - 水平扩展: 每个JVM实例独立处理
 */
@ApplicationScoped
public class KpiSQLiteEngine extends AbstractKpiQueryEngineImpl {

    private static final Logger log = LoggerFactory.getLogger(KpiSQLiteEngine.class);
    private static final String NOT_EXISTS = "--";

    @Inject
    SQLiteFileManager sqliteFileManager;

    @Inject
    @io.quarkus.agroal.DataSource("sqlite")
    AgroalDataSource sqliteDataSource;

    @Override
    protected String getKpiDataTableName(String kpiId, String cycleType, String compDimCode, String opTime) {
        return sqliteFileManager.getSQLiteTableName(kpiId, opTime, compDimCode);
    }

    @Override
    protected String getDimDataTableName(String compDimCode) {
        // 维度表命名规则：dim_{compDimCode}
        return String.format("kpi_dim_%s", compDimCode);
    }

    @Override
    protected Connection getSQLiteConnection(KpiQueryRequest request) throws SQLException, IOException {
        String uniqueId = UUID.randomUUID().toString().replace("-", "");
        return DriverManager.getConnection("jdbc:sqlite:file:memdb_" + uniqueId + "?mode=memory");
    }

    @Override
    protected void preQuery(KpiQueryRequest request, Connection conn) throws Exception {

        Map<String, KpiDefinition> kpiDefinitions = metadataRepository.batchGetKpiDefinitions(request.kpiArray());
        Set<String> addedCompDims = new HashSet<>();
        Map<String, KpiDefinition> parsedKpiDefinitions = parseKpiDependencies(kpiDefinitions);
        // 根据请求，装载所有需要的表

        // 每个指标
        for (Map.Entry<String, KpiDefinition> entry : parsedKpiDefinitions.entrySet()) {
            String kpiId = entry.getKey();
            KpiDefinition kpiDefinition = entry.getValue();
            String compDimCode = kpiDefinition.compDimCode();
            // 每个时间
            for (String opTime : request.opTimeArray()) {
                addDataTable(conn, opTime, kpiId, compDimCode);
            }

            // 如果要查询历史同比环比
            if(request.includeHistoricalData()) {
                for (String opTime : request.opTimeArray()) {
                    String lastCycle = calculateLastCycleTime(opTime);
                    String lastYear = calculateLastYearTime(opTime);
                    addDataTable(conn, lastCycle, kpiId, compDimCode);
                    addDataTable(conn, lastYear, kpiId, compDimCode);
                }
            }

            // 每个维度
            if(!addedCompDims.contains(compDimCode)) {
                addDimTable(conn, compDimCode);
                addedCompDims.add(compDimCode);
            }
        }
        // 如果要查询目标值
        if(request.includeTargetData()) {
            for (String compDimCode : addedCompDims) {
                addTargetTable(conn, compDimCode);
            }
        }
    }

    private void addTargetTable(Connection conn, String compDimCode) throws IOException, SQLException {
        try{
            String localPath = sqliteFileManager.downloadAndCacheTargetDB(compDimCode);
            try( Statement stmt = conn.createStatement();){
                stmt.execute("ATTACH '"+localPath+"' as temp_target_db");
                String tableName = sqliteFileManager.getSQLiteTargetTableName(compDimCode);
                stmt.execute("create table "+ tableName + " as select * from temp_target_db."+tableName);
                log.debug("添加目标值表至SQLite: {}", tableName);
            }
        }catch (RuntimeException e){
            if(e.getMessage()!=null && e.getMessage().contains(SQLiteFileManager.S3_FILE_NOT_EXISTS)){
                createEmptyTargetTable(conn, compDimCode);
            }else{
                throw e;
            }
        }
    }

    private void createEmptyTargetTable(Connection conn, String compDimCode) throws SQLException{
        try (Statement stmt = conn.createStatement();){
            String tableName = sqliteFileManager.getSQLiteTargetTableName(compDimCode);
            List<DimDef> dims = metadataRepository.getDimDefsByCompDim(compDimCode);
            String dimDef = dims.stream().map(dim -> dim.dbColName() + " varchar(32) ").collect(Collectors.joining(","));

            stmt.execute("""
                create table %s (
                op_time varchar(32),
                kpi_id varchar(32),
                %s,
                target_value varchar(32),
                check_result varchar(64),
                check_desc varchar(512),
                eff_start_date datetime,
                eff_end_date datetime
                )
            """.formatted(tableName, dimDef));
        }
    }

    private void addDimTable(Connection conn, String compDimCode) throws IOException, SQLException {
        try{
            String localPath = sqliteFileManager.downloadAndCacheDimDB(compDimCode);
            try (Statement stmt = conn.createStatement();){
                stmt.execute("ATTACH '" + localPath + "' as temp_dim_db");
                String tableName = sqliteFileManager.getSQLiteDimTableName(compDimCode);
                stmt.execute("create table "+ tableName + " as select * from temp_dim_db."+tableName);
                log.debug("添加维度表至SQLite: {}", tableName);
                stmt.execute("DETACH temp_dim_db");
            }
        }catch (RuntimeException e){
            if(e.getMessage()!=null && e.getMessage().contains(SQLiteFileManager.S3_FILE_NOT_EXISTS)){
                createEmptyDimTable(conn,  compDimCode);
            }else{
                throw e;
            }
        }
    }

    private void createEmptyDimTable(Connection conn, String compDimCode) throws SQLException{
        try (Statement stmt = conn.createStatement();) {
            String tableName = sqliteFileManager.getSQLiteDimTableName(compDimCode);
            stmt.execute("""
            create table %s (
                dim_code        varchar(32),
                dim_val         varchar(128),
                dim_id          varchar(32),
                parent_dim_code varchar(32)
            )
            """.formatted(tableName));
        }
    }

    private void createEmptyDataTable(Connection conn, String opTime, String kpiId, String compDimCode) throws SQLException {
        try (Statement stmt = conn.createStatement();){
            String tableName = sqliteFileManager.getSQLiteTableName(kpiId, opTime, compDimCode);
            List<DimDef> dims = metadataRepository.getDimDefsByCompDim(compDimCode);
            String dimDef = dims.stream().map(dim -> dim.dbColName() + " varchar(32) ").collect(Collectors.joining(","));

            stmt.execute("""
                create table %s (
                op_time varchar(32),
                kpi_id varchar(32),
                %s,
                kpi_val varchar(32)
                )
            """.formatted(tableName, dimDef));
        }
    }

    private void addDataTable(Connection conn, String opTime, String kpiId, String compDimCode) throws IOException, SQLException {
        try{
            String localPath = sqliteFileManager.downloadDataDB(kpiId, opTime, compDimCode);
            try (Statement stmt = conn.createStatement();){
                stmt.execute("ATTACH '" + localPath + "' as temp_data_db");
                String tableName = sqliteFileManager.getSQLiteTableName(kpiId, opTime, compDimCode);
                stmt.execute("create table "+ tableName + " as select * from temp_data_db."+tableName);
                log.debug("添加数据表至SQLite: {}", tableName);
                stmt.execute("DETACH temp_data_db");
            }
        }catch (RuntimeException e){ // 不存在时，创建一个空表保证SQL正常执行
            if(e.getMessage()!=null && e.getMessage().contains(SQLiteFileManager.S3_FILE_NOT_EXISTS)){
                createEmptyDataTable(conn, opTime, kpiId, compDimCode);
            }else{
                throw e;
            }

        }
    }
}
