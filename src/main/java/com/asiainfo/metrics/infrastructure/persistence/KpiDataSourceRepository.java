package com.asiainfo.metrics.infrastructure.persistence;

import com.asiainfo.metrics.shared.util.AesCipher;
import io.agroal.api.AgroalDataSource;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

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

    @ConfigProperty(name = "metrics.driver.plugin.dir")
    private String driverPluginDir;


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

                    // 使用自定义类加载器加载驱动
                    ClassLoader extendedClassLoader = createExtendedClassLoader();
                    Class<?> driverClass = Class.forName(driverClassName, true, extendedClassLoader);

                    // 创建驱动实例
                    java.sql.Driver driver = (java.sql.Driver) driverClass.getDeclaredConstructor().newInstance();

                    // 创建连接属性
                    java.util.Properties props = new java.util.Properties();
                    props.setProperty("user", userName);
                    props.setProperty("password", AesCipher.decrypt(password));

                    // 使用驱动实例获取连接
                    Connection connection = driver.connect(url, props);
                    connection.prepareStatement(validationSql).execute();
                    return connection;
                }
                throw new RuntimeException("指定的数据源不存在:" + dsName);
            } catch (ClassNotFoundException e) {
                throw new RuntimeException("加载驱动失败: " + e.getMessage(), e);
            } catch (InstantiationException | IllegalAccessException | NoSuchMethodException |
                     java.lang.reflect.InvocationTargetException e) {
                throw new RuntimeException("创建驱动实例失败: " + e.getMessage(), e);
            }

        } catch (SQLException e) {
            throw new RuntimeException("查询数据源失败: " + dsName, e);
        }
    }

    private ClassLoader createExtendedClassLoader() {
        try {
            Path driverDir = Paths.get(driverPluginDir);
            if (!Files.exists(driverDir)) {
                return Thread.currentThread().getContextClassLoader();
            }

            List<URL> urls = new ArrayList<>();
            Files.list(driverDir)
                    .filter(path -> path.toString().endsWith(".jar"))
                    .forEach(jarPath -> {
                        try {
                            urls.add(jarPath.toUri().toURL());
                        } catch (Exception e) {
                            System.err.println("无法加载JAR: " + jarPath);
                        }
                    });

            if (urls.isEmpty()) {
                return Thread.currentThread().getContextClassLoader();
            }

            return new URLClassLoader(urls.toArray(new URL[0]),
                    Thread.currentThread().getContextClassLoader());
        } catch (Exception e) {
            return Thread.currentThread().getContextClassLoader();
        }
    }

}
