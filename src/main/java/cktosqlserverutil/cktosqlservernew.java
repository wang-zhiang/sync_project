package sqlservertockutil.sync;

import au.com.bytecode.opencsv.CSVReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class cktosqlservernew {
    private static final String CLICKHOUSE_URL = "jdbc:clickhouse://hadoop110:8123/ods";
    private static final String CLICKHOUSE_USER = "default";
    private static final String CLICKHOUSE_PASSWORD = "smartpath";
    private static final String SQL_SERVER_URL = "jdbc:sqlserver://192.168.3.182;DatabaseName=SyncWebSearchJD";
    private static final String SQL_SERVER_USER = "sa";
    private static final String SQL_SERVER_PASSWORD = "smartpthdata";

    private static final int BATCH_SIZE = 30000;
    private static final int PAGE_SIZE = 50000;

    public static void main(String[] args) {
        System.out.println("Starting synchronization process...");
        Map<String, String> tableMappings = readCSV("D:\\wzza\\develop\\idea_project\\ceshi\\src\\main\\java\\cktosqlserverutil\\a.csv");
        String whereCondition = "length(itemid) < 43";

        ExecutorService executorService = Executors.newFixedThreadPool(5);

        for (Map.Entry<String, String> entry : tableMappings.entrySet()) {
            String clickHouseTableName = "ods." + entry.getKey().trim();
            String sqlServerTableName = entry.getValue().trim();

            int totalRowCountClickHouse = getTotalRowCount(clickHouseTableName, whereCondition);
            if (totalRowCountClickHouse == -1) {
                System.err.println("Failed to retrieve total row count for table: " + clickHouseTableName);
                continue;
            }
            System.out.println("Total rows to sync1 from ClickHouse table " + clickHouseTableName + ": " + totalRowCountClickHouse);

            if (!createSQLServerTable(sqlServerTableName, clickHouseTableName)) {
                System.err.println("Failed to create table in SQL Server for " + sqlServerTableName);
                continue;
            }

            int partitions = (totalRowCountClickHouse + PAGE_SIZE - 1) / PAGE_SIZE;
            for (int i = 0; i < partitions; i++) {
                int offset = i * PAGE_SIZE;
                System.out.println("Submitting task for table: " + clickHouseTableName + ", offset: " + offset);
                executorService.submit(() -> insertPartition(clickHouseTableName, sqlServerTableName, offset, whereCondition));
            }

            executorService.shutdown();
            try {
                if (!executorService.awaitTermination(1, TimeUnit.HOURS)) {
                    System.err.println("Tasks did not finish in the expected time.");
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                e.printStackTrace();
            }

            int totalRowCountSQLServer = getTotalRowCountSQLServer(sqlServerTableName);
            System.out.println("Total rows in SQL Server table " + sqlServerTableName + ": " + totalRowCountSQLServer);
            if (totalRowCountSQLServer == totalRowCountClickHouse) {
                System.out.println("Synchronization successful for table " + clickHouseTableName + ".");
            } else {
                System.err.println("Synchronization mismatch for table " + clickHouseTableName + ": expected "
                        + totalRowCountClickHouse + ", but got " + totalRowCountSQLServer);
            }
        }

        System.out.println("Synchronization process completed.");
    }

    private static int getTotalRowCount(String clickHouseTableName, String whereCondition) {
        String countQuery = String.format("SELECT COUNT(*) FROM %s WHERE %s", clickHouseTableName, whereCondition);
        try (Connection connection = DriverManager.getConnection(CLICKHOUSE_URL, CLICKHOUSE_USER, CLICKHOUSE_PASSWORD);
             Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery(countQuery)) {
            if (resultSet.next()) return resultSet.getInt(1);
        } catch (SQLException e) {
            System.err.println("Error fetching total row count for " + clickHouseTableName + ": " + e.getMessage());
        }
        return -1;
    }

    private static int getTotalRowCountSQLServer(String sqlServerTableName) {
        String countQuery = "SELECT COUNT(*) FROM " + sqlServerTableName;
        try (Connection connection = DriverManager.getConnection(SQL_SERVER_URL, SQL_SERVER_USER, SQL_SERVER_PASSWORD);
             Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery(countQuery)) {
            if (resultSet.next()) return resultSet.getInt(1);
        } catch (SQLException e) {
            System.err.println("Error fetching total row count for " + sqlServerTableName + ": " + e.getMessage());
        }
        return -1;
    }

    private static boolean createSQLServerTable(String sqlServerTableName, String clickHouseTableName) {
        try (Connection clickHouseConnection = DriverManager.getConnection(CLICKHOUSE_URL, CLICKHOUSE_USER, CLICKHOUSE_PASSWORD);
             Connection sqlServerConnection = DriverManager.getConnection(SQL_SERVER_URL, SQL_SERVER_USER, SQL_SERVER_PASSWORD);
             Statement clickHouseStmt = clickHouseConnection.createStatement();
             ResultSet resultSet = clickHouseStmt.executeQuery("SELECT * FROM " + clickHouseTableName + " WHERE 1 = 0")) {

            ResultSetMetaData metaData = resultSet.getMetaData();
            StringBuilder createTableQuery = new StringBuilder("IF OBJECT_ID(N'").append(sqlServerTableName)
                    .append("', N'U') IS NULL CREATE TABLE ").append(sqlServerTableName).append(" (");

            for (int i = 1; i <= metaData.getColumnCount(); i++) {
                String columnName = metaData.getColumnName(i);
                String sqlServerType = mapClickHouseTypeToSqlType(metaData.getColumnTypeName(i));
                createTableQuery.append(columnName).append(" ").append(sqlServerType);
                if (i < metaData.getColumnCount()) createTableQuery.append(", ");
            }
            createTableQuery.append(");");

            try (Statement sqlServerStmt = sqlServerConnection.createStatement()) {
                sqlServerStmt.execute(createTableQuery.toString());
                System.out.println("Table created in SQL Server: " + sqlServerTableName);
            }
            return true;
        } catch (SQLException e) {
            System.err.println("Error creating table in SQL Server: " + e.getMessage());
            return false;
        }
    }

    private static void insertPartition(String clickHouseTableName, String sqlServerTableName, int offset, String whereCondition) {
        String clickHouseQuery = String.format("SELECT * FROM %s WHERE %s LIMIT %d OFFSET %d", clickHouseTableName, whereCondition, PAGE_SIZE, offset);
        System.out.println("Executing ClickHouse query: " + clickHouseQuery);

        try (Connection clickHouseConnection = DriverManager.getConnection(CLICKHOUSE_URL, CLICKHOUSE_USER, CLICKHOUSE_PASSWORD);
             PreparedStatement clickHouseStmt = clickHouseConnection.prepareStatement(clickHouseQuery);
             ResultSet resultSet = clickHouseStmt.executeQuery()) {

            if (!resultSet.next()) {
                System.out.println("No data returned for offset " + offset + " in table " + clickHouseTableName);
                return;
            }
            resultSet.beforeFirst();

            try (Connection sqlServerConnection = DriverManager.getConnection(SQL_SERVER_URL, SQL_SERVER_USER, SQL_SERVER_PASSWORD);
                 PreparedStatement sqlServerPreparedStatement = prepareSQLServerInsert(sqlServerTableName, sqlServerConnection, resultSet)) {

                int count = 0;
                while (resultSet.next()) {
                    System.out.println("Preparing to insert row with data:");
                    for (int j = 1; j <= resultSet.getMetaData().getColumnCount(); j++) {
                        String value = resultSet.getString(j);
                        System.out.print("[" + j + "]: " + value + " ");
                        sqlServerPreparedStatement.setString(j, value);
                    }
                    System.out.println("\nBatching insert.");
                    sqlServerPreparedStatement.addBatch();

                    if (++count % BATCH_SIZE == 0) {
                        System.out.println("Executing batch of size " + BATCH_SIZE);
                        sqlServerPreparedStatement.executeBatch();
                        System.out.println("Inserted " + count + " rows into SQL Server for offset " + offset + ".");
                    }
                }
                if (count % BATCH_SIZE != 0) {
                    System.out.println("Executing final batch with " + (count % BATCH_SIZE) + " rows.");
                    sqlServerPreparedStatement.executeBatch();
                    System.out.println("Inserted " + count + " rows into SQL Server for offset " + offset + ".");
                }
            } catch (SQLException e) {
                System.err.println("SQL Server insert error for offset " + offset + ": " + e.getMessage());
                e.printStackTrace();
            }
        } catch (SQLException e) {
            System.err.println("ClickHouse query execution error for offset " + offset + ": " + e.getMessage());
            e.printStackTrace();
        }
    }

    private static PreparedStatement prepareSQLServerInsert(String sqlServerTableName, Connection sqlServerConnection, ResultSet resultSet) throws SQLException {
        ResultSetMetaData metaData = resultSet.getMetaData();
        StringBuilder columns = new StringBuilder();
        StringBuilder placeholders = new StringBuilder();

        for (int i = 1; i <= metaData.getColumnCount(); i++) {
            columns.append(metaData.getColumnName(i));
            placeholders.append("?");
            if (i < metaData.getColumnCount()) {
                columns.append(", ");
                placeholders.append(", ");
            }
        }
        String insertQuery = String.format("INSERT INTO %s (%s) VALUES (%s)", sqlServerTableName, columns, placeholders);
        System.out.println("Prepared SQL insert statement: " + insertQuery);
        return sqlServerConnection.prepareStatement(insertQuery);
    }

    private static String mapClickHouseTypeToSqlType(String clickHouseType) {
        switch (clickHouseType.toUpperCase()) {
            case "INT32":
            case "INT":
                return "INT";
            case "INT64":
            case "BIGINT":
                return "BIGINT";
            case "FLOAT32":
            case "FLOAT64":
                return "FLOAT";
            case "STRING":
                return "NVARCHAR(MAX)";
            case "DATE":
                return "DATE";
            case "DATETIME":
                return "DATETIME";
            default:
                return "NVARCHAR(MAX)";
        }
    }

    private static Map<String, String> readCSV(String filePath) {
        Map<String, String> tableMappings = new HashMap<>();
        try (CSVReader reader = new CSVReader(new InputStreamReader(new FileInputStream(filePath), StandardCharsets.UTF_8), ',')) {
            String[] line;
            while ((line = reader.readNext()) != null) {
                if (line.length < 2) continue;
                tableMappings.put(line[0].trim(), line[1].trim());
            }
        } catch (IOException e) {
            System.err.println("Error reading CSV file: " + e.getMessage());
        }
        return tableMappings;
    }
}
