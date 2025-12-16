package kzb;

import ru.yandex.clickhouse.ClickHouseConnection;
import ru.yandex.clickhouse.ClickHouseDataSource;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class ClickHouseUtil {

    public static ClickHouseConnection getConnection(String url) throws SQLException {
        ClickHouseDataSource dataSource = new ClickHouseDataSource(url);
        return dataSource.getConnection();
    }

    public static void executeUpdate(String url, String sql) throws SQLException {
        try (ClickHouseConnection conn = getConnection(url);
             Statement stmt = conn.createStatement()) {
            stmt.executeUpdate(sql);
        }
    }
    
    // 新增查询方法
    public static long executeCountQuery(String url, String sql) throws SQLException {
        try (ClickHouseConnection conn = getConnection(url);
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            if (rs.next()) {
                return rs.getLong(1);
            }
            return 0;
        }
    }
}