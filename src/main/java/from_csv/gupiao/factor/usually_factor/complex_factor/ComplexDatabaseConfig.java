package from_csv.gupiao.factor.usually_factor.complex_factor;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * 复杂因子计算的数据库配置类
 */
public class ComplexDatabaseConfig {
    private static final String URL = "jdbc:clickhouse://hadoop110:8123/default?socket_timeout=300000&connection_timeout=10000&compress=1";
    private static final String USER = "default";
    private static final String PASSWORD = "smartpath";
    
    static {
        try {
            // 尝试新版本驱动
            Class.forName("com.clickhouse.jdbc.ClickHouseDriver");
        } catch (ClassNotFoundException e1) {
            try {
                // 如果新版本不存在，尝试旧版本驱动
                Class.forName("ru.yandex.clickhouse.ClickHouseDriver");
                System.out.println("使用旧版本ClickHouse驱动: ru.yandex.clickhouse.ClickHouseDriver");
            } catch (ClassNotFoundException e2) {
                throw new RuntimeException("ClickHouse JDBC driver not found. 请添加ClickHouse JDBC依赖到项目中。\n" +
                    "Maven: <dependency><groupId>com.clickhouse</groupId><artifactId>clickhouse-jdbc</artifactId><version>0.4.6</version></dependency>\n" +
                    "或者尝试旧版本: <dependency><groupId>ru.yandex.clickhouse</groupId><artifactId>clickhouse-jdbc</artifactId><version>0.3.2</version></dependency>", e2);
            }
        }
    }
    
    public static Connection getConnection() throws SQLException {
        return DriverManager.getConnection(URL, USER, PASSWORD);
    }
}