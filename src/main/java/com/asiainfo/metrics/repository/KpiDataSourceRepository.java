package com.asiainfo.metrics.repository;

import io.agroal.api.AgroalDataSource;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import java.sql.*;

/**
 *
 *
 * @author QvQ
 * @date 2025/11/12
 */
@ApplicationScoped
public class KpiDataSourceRepository {

    @Inject
    @io.quarkus.agroal.DataSource("metadb")
    AgroalDataSource metadbDataSource;


    public Connection getConnection(String dsName) {
        String dsQuery = """
                select ds_acct, ds_auth, driver_class_name, url, validation_sql
                from modo_datasource
                where ds_name = ?
                """;

        try (Connection conn = metadbDataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(dsQuery)) {

            stmt.setString(1, dsName);

            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    String url = rs.getString("url");
                    String userName = rs.getString("ds_acct");
                    String password = rs.getString("ds_auth");
                    String driverClassName = rs.getString("driver_class_name");
                    String validationSql = rs.getString("validation_sql");
                    Class.forName(driverClassName);
                    Connection connection = DriverManager.getConnection(url, userName, password);
                    connection.prepareStatement(validationSql).execute();
                    return connection;
                }
                throw new RuntimeException("指定的数据源不存在:" + dsName);
            } catch (ClassNotFoundException e) {
                throw new RuntimeException(e);
            }

        } catch (SQLException e) {
            throw new RuntimeException("查询数据源失败: " + dsName, e);
        }
    }

}
