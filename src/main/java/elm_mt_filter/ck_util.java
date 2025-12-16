package elm_mt_filter;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

public class ck_util {

    private static final String CLICKHOUSE_URL = "jdbc:clickhouse://hadoop110:8123?socket_timeout=600000";
    private static final String CLICKHOUSE_USERNAME = "default";
    private static final String CLICKHOUSE_PASSWORD = "smartpath";

    //注意查询语句要带上库名
    public static void ckEXECUTE(String clickHouseCreateTableQuery) {
        try {
            Connection clickHouseConnection = DriverManager.getConnection(CLICKHOUSE_URL, CLICKHOUSE_USERNAME, CLICKHOUSE_PASSWORD);
            Statement clickHouseStatement = clickHouseConnection.createStatement();
            clickHouseStatement.execute(clickHouseCreateTableQuery);

        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
