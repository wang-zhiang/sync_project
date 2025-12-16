package mysqlutil;

import ru.yandex.clickhouse.ClickHouseConnection;
import ru.yandex.clickhouse.ClickHouseDataSource;
import ru.yandex.clickhouse.settings.ClickHouseProperties;

import java.sql.*;
import java.util.LinkedHashSet;
import java.util.Set;

public class mysql_ck_for_i_retry {

    private static final int BATCH_SIZE = 10000; // 每批处理的数据量
    private static final int MAX_RETRIES = 5; // 最大重试次数
    private static final long RETRY_INTERVAL = 600000; // 重试间隔（毫秒）

    public static void main(String[] args) throws InterruptedException {
        try {
            migrateData();
        } catch (Exception e) {
            System.err.println("Migration failed: " + e.getMessage());
            e.printStackTrace();
        }
    }

    private static void migrateData() throws InterruptedException {
        String MYSQL_JDBC_URL = "jdbc:mysql://192.168.5.34:3306/elm202111?socketTimeout=1800000";
        String MYSQL_USER = "root";
        String MYSQL_PASSWORD = "smartpthdata";
        String CLICKHOUSE_JDBC_URL = "jdbc:clickhouse://hadoop110:8123/test";
        String CLICKHOUSE_USER = "default";
        String CLICKHOUSE_PASSWORD = "smartpath";
        String MYSQL_TABLE = "goods_supermarket";
        String CLICKHOUSE_TABLE = "test.elmmarket_2111";

        ClickHouseProperties properties = new ClickHouseProperties();
        properties.setUser(CLICKHOUSE_USER);
        properties.setPassword(CLICKHOUSE_PASSWORD);
        ClickHouseDataSource clickHouseDataSource = new ClickHouseDataSource(CLICKHOUSE_JDBC_URL, properties);

        int attempts = 0;
        boolean migrationCompleted = false;
        while (!migrationCompleted && attempts < MAX_RETRIES) {
            try (Connection mysqlConn = DriverManager.getConnection(MYSQL_JDBC_URL, MYSQL_USER, MYSQL_PASSWORD);
                 Connection clickhouseConn = clickHouseDataSource.getConnection()) {

                System.out.println("Starting data migration from MySQL to ClickHouse...");
                System.out.println("Connected to MySQL and ClickHouse databases.");

                System.out.println("Starting data migration from MySQL to ClickHouse...");
                System.out.println("Connected to MySQL and ClickHouse databases.");

                // Assuming 'id' as the primary key for the sake of example
                String primaryKeyColumn = "id";

                // Insert data logic starts here
                // Example of fetching data from MySQL and preparing for ClickHouse insertion
                String shardSelectQuery = "SELECT * FROM " + MYSQL_TABLE;
                PreparedStatement mysqlSelectStmt = mysqlConn.prepareStatement(shardSelectQuery);
                ResultSet mysqlDataRs = mysqlSelectStmt.executeQuery();

                // Construct the insert SQL statement dynamically based on columns fetched from MySQL
                String insertQuery = constructInsertQuery(CLICKHOUSE_TABLE, mysqlDataRs);
                PreparedStatement clickhouseInsertStmt = clickhouseConn.prepareStatement(insertQuery);

                int batchSize = 0;
                while (mysqlDataRs.next()) {
                    // Assuming a simple mapping for demonstration. Adjust based on actual schema.
                    // This is where you map the MySQL data to ClickHouse insert statement
                    prepareInsertStatement(clickhouseInsertStmt, mysqlDataRs);

                    clickhouseInsertStmt.addBatch();
                    if (++batchSize % BATCH_SIZE == 0) {
                        clickhouseInsertStmt.executeBatch();
                        System.out.println("Inserted batch of " + batchSize + " records.");
                    }
                }

                // Execute any remaining batches
                if (batchSize % BATCH_SIZE != 0) {
                    clickhouseInsertStmt.executeBatch();
                    System.out.println("Inserted final batch of records.");
                }

                System.out.println("Data migration completed successfully.");
                migrationCompleted = true; // Mark migration as completed
            } catch (SQLException e) {
                System.err.println("Attempt " + (attempts + 1) + " failed: " + e.getMessage());
                if (++attempts >= MAX_RETRIES) {
                    System.err.println("Migration failed after " + MAX_RETRIES + " attempts.");
                    break;
                }
                System.out.println("Retrying after 10 minutes...");
                Thread.sleep(RETRY_INTERVAL);
            }
        }
    }

    private static String constructInsertQuery(String clickhouseTable, ResultSet mysqlDataRs) throws SQLException {
        ResultSetMetaData metaData = mysqlDataRs.getMetaData();
        int columnCount = metaData.getColumnCount();
        StringBuilder queryBuilder = new StringBuilder("INSERT INTO " + clickhouseTable + " (");

        for (int i = 1; i <= columnCount; i++) {
            queryBuilder.append(metaData.getColumnName(i).toLowerCase());
            if (i < columnCount) queryBuilder.append(", ");
        }

        queryBuilder.append(") VALUES (");
        for (int i = 1; i <= columnCount; i++) {
            queryBuilder.append("?");
            if (i < columnCount) queryBuilder.append(", ");
        }
        queryBuilder.append(")");

        return queryBuilder.toString();
    }

    private static void prepareInsertStatement(PreparedStatement clickhouseInsertStmt, ResultSet mysqlDataRs) throws SQLException {
        ResultSetMetaData metaData = mysqlDataRs.getMetaData();
        int columnCount = metaData.getColumnCount();

        for (int i = 1; i <= columnCount; i++) {
            Object value = mysqlDataRs.getObject(i);
            clickhouseInsertStmt.setObject(i, value);
        }
    }

    private static String mapDataType(String mysqlDataType) {
        // DataType mapping logic remains unchanged
        switch (mysqlDataType) {
            case "varchar":
            case "char":
            case "text":
            case "tinytext":
            case "mediumtext":
            case "longtext":
                return "String";
            case "blob":
            case "mediumblob":
            case "longblob":
            case "tinyblob":
            case "binary":
            case "varbinary":
                return "String";
            case "int":
            case "integer":
            case "mediumint":
            case "smallint":
            case "tinyint":
                return "Int32";
            case "bigint":
                return "Int64";
            case "float":
                return "Float32";
            case "double":
                return "Float64";
            case "decimal":
                return "Float64";
            case "date":
                return "Date";
            case "datetime":
            case "timestamp":
                return "DateTime";
            case "time":
                return "String";
            case "year":
                return "Int16";
            case "enum":
            case "set":
                return "String";
            case "json":
                return "String";
            case "bit":
                return "UInt8";
            default:
                return "String";
        }
    }
}
