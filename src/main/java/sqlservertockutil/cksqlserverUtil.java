package sqlservertockutil;

import ru.yandex.clickhouse.ClickHouseDataSource;
import ru.yandex.clickhouse.settings.ClickHouseProperties;

import java.sql.*;

public class cksqlserverUtil {

    private static final String CLICKHOUSE_URL = "jdbc:clickhouse://hadoop110:8123";
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

    //这个方法是用来给ck查同步条数，qty 以及price的
    public static ResultSet executeQuery(String query) throws SQLException {
        ClickHouseProperties properties = new ClickHouseProperties();
        properties.setUser(CLICKHOUSE_USERNAME);
        properties.setPassword(CLICKHOUSE_PASSWORD);

        ClickHouseDataSource dataSource = new ClickHouseDataSource(CLICKHOUSE_URL, properties);

        Connection connection = dataSource.getConnection();
        Statement statement = connection.createStatement();

        return statement.executeQuery(query);
    }











//    public static void main(String[] args) {
//        insertafterdelete("1","test","AllenCSTBckDB");
//
//    }







}
