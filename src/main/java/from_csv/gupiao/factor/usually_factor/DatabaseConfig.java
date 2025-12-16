package from_csv.gupiao.factor.usually_factor;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class DatabaseConfig {
    // TODO: 请填入您的ClickHouse连接信息
    private static final String URL = "jdbc:clickhouse://hadoop110:8123/ods";
    private static final String USERNAME = "default";
    private static final String PASSWORD = "smartpath";
    
    static {
        try {
            Class.forName("ru.yandex.clickhouse.ClickHouseDriver");
        } catch (ClassNotFoundException e) {
            throw new RuntimeException("ClickHouse JDBC driver not found", e);
        }
    }
    
    public static Connection getConnection() throws SQLException {
        return DriverManager.getConnection(URL, USERNAME, PASSWORD);
    }
}