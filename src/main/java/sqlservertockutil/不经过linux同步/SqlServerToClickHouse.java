package sqlservertockutil.不经过linux同步;

import java.sql.*;

//单独的java同步， 需保证ck和sqlserver字段相同且没有中文的字段
public class SqlServerToClickHouse {
    // SQL Server connection details
    private static final String SQL_SERVER_URL = "jdbc:sqlserver://192.168.99.31:2722;DatabaseName=SearchCommentY264";
    private static final String SQL_SERVER_USER = "sa";
    private static final String SQL_SERVER_PASSWORD = "smartpthdata";
    private static final String SQL_SERVER_QUERY = "SELECT * FROM DDDD案例一lx"; // or a custom query

    // ClickHouse connection details
    private static final String CLICKHOUSE_URL = "jdbc:clickhouse://hadoop110:8123/wph";
    private static final String CLICKHOUSE_USER = "default";
    private static final String CLICKHOUSE_PASSWORD = "smartpath";
    private static final String CLICKHOUSE_TABLE = "items264_2404";

    // Batch size for insertion
    private static final int BATCH_SIZE = 30000;

    public static void main(String[] args) {
        Connection sqlServerConnection = null;
        Connection clickHouseConnection = null;
        Statement sqlServerStatement = null;
        PreparedStatement clickHousePreparedStatement = null;
        ResultSet resultSet = null;

        try {
            // Step 1: Connect to SQL Server
            sqlServerConnection = DriverManager.getConnection(SQL_SERVER_URL, SQL_SERVER_USER, SQL_SERVER_PASSWORD);
            sqlServerStatement = sqlServerConnection.createStatement();
            resultSet = sqlServerStatement.executeQuery(SQL_SERVER_QUERY);

            // Get metadata for column count and names
            ResultSetMetaData metaData = resultSet.getMetaData();
            int columnCount = metaData.getColumnCount();
            StringBuilder columns = new StringBuilder();
            StringBuilder placeholders = new StringBuilder();

            for (int i = 1; i <= columnCount; i++) {
                columns.append(metaData.getColumnName(i));
                placeholders.append("?");
                if (i < columnCount) {
                    columns.append(", ");
                    placeholders.append(", ");
                }
            }

            String clickHouseInsertQuery = String.format("INSERT INTO %s (%s) VALUES (%s)", CLICKHOUSE_TABLE, columns, placeholders);

            // Step 2: Connect to ClickHouse
            clickHouseConnection = DriverManager.getConnection(CLICKHOUSE_URL, CLICKHOUSE_USER, CLICKHOUSE_PASSWORD);
            clickHousePreparedStatement = clickHouseConnection.prepareStatement(clickHouseInsertQuery);

            // Step 3: Process each row from SQL Server and insert into ClickHouse
            int count = 0;
            while (resultSet.next()) {
                for (int i = 1; i <= columnCount; i++) {
                    String value = resultSet.getString(i);
                    if (value != null) {
                        value = value.replace("\n", "").replace("\t", "").replace("\r", "");
                    }
                    clickHousePreparedStatement.setString(i, value);
                }
                clickHousePreparedStatement.addBatch();
                count++;

                if (count % BATCH_SIZE == 0) {
                    clickHousePreparedStatement.executeBatch();
                    System.out.println("Inserted " + count + " rows into ClickHouse.");
                }
            }

            // Execute the remaining batch
            if (count % BATCH_SIZE != 0) {
                clickHousePreparedStatement.executeBatch();
                System.out.println("Inserted " + count + " rows into ClickHouse.");
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            // Close resources
            try {
                if (resultSet != null) resultSet.close();
                if (sqlServerStatement != null) sqlServerStatement.close();
                if (sqlServerConnection != null) sqlServerConnection.close();
                if (clickHousePreparedStatement != null) clickHousePreparedStatement.close();
                if (clickHouseConnection != null) clickHouseConnection.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }
}
