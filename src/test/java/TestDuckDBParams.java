import java.sql.*;
import java.util.Properties;

public class TestDuckDBParams {
    public static void main(String[] args) throws Exception {
        Class.forName("org.duckdb.DuckDBDriver");
        try (Connection conn = DriverManager.getConnection("jdbc:duckdb:")) {
            Statement stmt = conn.createStatement();
            stmt.execute("COPY (SELECT 1 as id, 'test' as name) TO 'test.parquet' (FORMAT PARQUET)");

            String sql = "SELECT * FROM read_parquet(?)";
            try (PreparedStatement ps = conn.prepareStatement(sql)) {
                ps.setString(1, "test.parquet");
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        System.out.println("ID: " + rs.getInt("id") + ", Name: " + rs.getString("name"));
                    }
                }
            }
        }
    }
}
