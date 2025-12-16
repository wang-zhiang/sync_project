package hivesource;

import ru.yandex.clickhouse.ClickHouseDataSource;
import ru.yandex.clickhouse.settings.ClickHouseProperties;

import java.sql.*;

public class cksqlserverUtil {

    private static final String CLICKHOUSE_URL = "jdbc:clickhouse://hadoop110:8123";
    private static final String CLICKHOUSE_USERNAME = "default";
    private static final String CLICKHOUSE_PASSWORD = "smartpath";

    //注意查询语句要带上库名
    public static void ckEXECUTE(String clickHouseCreateTableQuery,String clickhouseDatabase,String clickhousetable) {
        try {
            Connection clickHouseConnection = DriverManager.getConnection(CLICKHOUSE_URL, CLICKHOUSE_USERNAME, CLICKHOUSE_PASSWORD);
            DatabaseMetaData metaData = clickHouseConnection.getMetaData();
            ResultSet tablesResultSet = metaData.getTables(null, clickhouseDatabase, clickhousetable, null);
            //如果表不存在
            if (!tablesResultSet.next()) {
                Statement clickHouseStatement = clickHouseConnection.createStatement();
                clickHouseStatement.execute(clickHouseCreateTableQuery);
                System.out.println("ClickHouse建表成功!");
            } else {
                System.out.println("该表已经存在");
            }
        } catch (SQLException e) {
           e.printStackTrace();
        }
    }

    public static void execute(String clickHouseCreateTableQuery) {
        try {
            Connection clickHouseConnection = DriverManager.getConnection(CLICKHOUSE_URL, CLICKHOUSE_USERNAME, CLICKHOUSE_PASSWORD);
            Statement clickHouseStatement = clickHouseConnection.createStatement();
            clickHouseStatement.execute(clickHouseCreateTableQuery);

        } catch (SQLException e) {
            e.printStackTrace();
        }
    }





//    public static void main(String[] args) {
//        insertafterdelete("1","test","AllenCSTBckDB");
//
//    }







}
