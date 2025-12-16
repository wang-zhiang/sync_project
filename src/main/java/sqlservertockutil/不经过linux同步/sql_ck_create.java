package sqlservertockutil.不经过linux同步;




import java.sql.*;

public class sql_ck_create {
    // SQL Server connection details
    private static final String SQL_SERVER_URL = "jdbc:sqlserver://192.168.99.31:2722;DatabaseName=SearchCommentY264";
    private static final String SQL_SERVER_USER = "sa";
    private static final String SQL_SERVER_PASSWORD = "smartpthdata";
    private static final String SQL_SERVER_QUERY_TEMPLATE = "SELECT * FROM trading2405原始 WHERE ABS(CHECKSUM(Id)) % 10 = ?"; // 分片查询模板

    // ClickHouse connection details
    private static final String CLICKHOUSE_URL = "jdbc:clickhouse://hadoop110:8123/wph";
    private static final String CLICKHOUSE_USER = "default";
    private static final String CLICKHOUSE_PASSWORD = "smartpath";
    private static final String CLICKHOUSE_TABLE = "wph.tradingOriginal264_2404";

    // Batch size for insertion
    private static final int BATCH_SIZE = 30000;

    public static void main(String[] args) {
        Connection sqlServerConnection = null;
        Connection clickHouseConnection = null;
        PreparedStatement sqlServerPreparedStatement = null;
        PreparedStatement clickHousePreparedStatement = null;
        ResultSet resultSet = null;

        int currentPartition = 0;
        int insertedRows = 0;

        try {
            // Step 1: Connect to SQL Server
            sqlServerConnection = DriverManager.getConnection(SQL_SERVER_URL, SQL_SERVER_USER, SQL_SERVER_PASSWORD);

            // Get metadata for column count and names
            Statement sqlServerStatement = sqlServerConnection.createStatement();
            resultSet = sqlServerStatement.executeQuery("SELECT * FROM trading2405原始 WHERE 1 = 0"); // 获取列信息
            ResultSetMetaData metaData = resultSet.getMetaData();
            int columnCount = metaData.getColumnCount();
            StringBuilder columns = new StringBuilder();
            StringBuilder placeholders = new StringBuilder();
            StringBuilder createTableQuery = new StringBuilder();
            String firstColumn = null;

            // Build the create table query for ClickHouse
            createTableQuery.append("CREATE TABLE IF NOT EXISTS ").append(CLICKHOUSE_TABLE).append(" (");

            for (int i = 1; i <= columnCount; i++) {
                String columnName = metaData.getColumnName(i);
                String pinyinColumnName = chinesetopinyin.chineseToPinyin(columnName); // 调用接口转换字段名
                String sqlType = metaData.getColumnTypeName(i);
                String clickHouseType = mapSqlTypeToClickHouseType(sqlType);

                if (i == 1) {
                    firstColumn = pinyinColumnName;
                }

                columns.append(pinyinColumnName);
                placeholders.append("?");
                createTableQuery.append(pinyinColumnName).append(" ").append(clickHouseType);

                if (i < columnCount) {
                    columns.append(", ");
                    placeholders.append(", ");
                    createTableQuery.append(", ");
                }
            }

            createTableQuery.append(") ENGINE = MergeTree() ORDER BY ").append(firstColumn).append(";");

            // Step 2: Connect to ClickHouse
            clickHouseConnection = DriverManager.getConnection(CLICKHOUSE_URL, CLICKHOUSE_USER, CLICKHOUSE_PASSWORD);
            Statement clickHouseStatement = clickHouseConnection.createStatement();

            // Create the table in ClickHouse if it doesn't exist
            try {
                clickHouseStatement.execute(createTableQuery.toString());
                System.out.println("Table created successfully in ClickHouse.");
            } catch (SQLException e) {
                System.err.println("Error creating table in ClickHouse: " + e.getMessage());
                return;
            }

            // Prepare the insert query for ClickHouse
            String clickHouseInsertQuery = String.format("INSERT INTO %s (%s) VALUES (%s)", CLICKHOUSE_TABLE, columns, placeholders);
            clickHousePreparedStatement = clickHouseConnection.prepareStatement(clickHouseInsertQuery);

            // Step 3: Process each row from SQL Server and insert into ClickHouse in partitions
            for (currentPartition = 0; currentPartition < 10; currentPartition++) {
                boolean success = false;
                while (!success) {
                    try {
                        sqlServerPreparedStatement = sqlServerConnection.prepareStatement(SQL_SERVER_QUERY_TEMPLATE);
                        sqlServerPreparedStatement.setInt(1, currentPartition);
                        resultSet = sqlServerPreparedStatement.executeQuery();

                        int count = 0;
                        while (resultSet.next()) {
                            for (int j = 1; j <= columnCount; j++) {
                                String value = resultSet.getString(j);
                                if (value != null) {
                                    // Handle DateTime format for ClickHouse
                                    if (metaData.getColumnTypeName(j).equalsIgnoreCase("DATETIME") || metaData.getColumnTypeName(j).equalsIgnoreCase("TIMESTAMP")) {
                                        value = value.substring(0, 19); // Truncate to "YYYY-MM-DD hh:mm:ss"
                                    }
                                    value = value.replace("\n", "").replace("\t", "").replace("\r", "");
                                }
                                clickHousePreparedStatement.setString(j, value);
                            }
                            clickHousePreparedStatement.addBatch();
                            count++;

                            if (count % BATCH_SIZE == 0) {
                                clickHousePreparedStatement.executeBatch();
                                System.out.println("Inserted " + count + " rows into ClickHouse for partition " + currentPartition + ".");
                            }
                        }

                        // Execute the remaining batch
                        if (count % BATCH_SIZE != 0) {
                            clickHousePreparedStatement.executeBatch();
                            System.out.println("Inserted " + count + " rows into ClickHouse for partition " + currentPartition + ".");
                        }
                        success = true;
                    } catch (SQLException e) {
                        System.err.println("Error inserting into ClickHouse: " + e.getMessage());
                        System.err.println("Retrying partition " + currentPartition + "...");
                        // Optionally, you can add a delay before retrying
                        try {
                            Thread.sleep(50000); // Wait for 5 seconds before retrying
                        } catch (InterruptedException ie) {
                            ie.printStackTrace();
                        }
                    }
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            // Close resources
            try {
                if (resultSet != null) resultSet.close();
                if (sqlServerPreparedStatement != null) sqlServerPreparedStatement.close();
                if (sqlServerConnection != null) sqlServerConnection.close();
                if (clickHousePreparedStatement != null) clickHousePreparedStatement.close();
                if (clickHouseConnection != null) clickHouseConnection.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    // Map SQL Server data types to ClickHouse data types
    private static String mapSqlTypeToClickHouseType(String sqlType) {
        switch (sqlType.toUpperCase()) {
            case "INT":
            case "INTEGER":
                return "Int32";
            case "BIGINT":
                return "Int64";
            case "SMALLINT":
                return "Int16";
            case "TINYINT":
                return "Int8";
            case "FLOAT":
                return "Float32";
            case "DOUBLE":
            case "REAL":
                return "Float64";
            case "DECIMAL":
            case "NUMERIC":
                return "Float64";
            case "BIT":
                return "UInt8";
            case "CHAR":
            case "NCHAR":
            case "VARCHAR":
            case "NVARCHAR":
            case "TEXT":
            case "NTEXT":
                return "String";
            case "DATE":
                return "Date";
            case "DATETIME":
            case "SMALLDATETIME":
            case "DATETIME2":
            case "TIMESTAMP":
                return "DateTime";
            default:
                return "String"; // Default to String if type is unknown
        }
    }
}
