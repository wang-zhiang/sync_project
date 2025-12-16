

package mysqlutil;

        import ru.yandex.clickhouse.ClickHouseConnection;
        import ru.yandex.clickhouse.ClickHouseDataSource;
        import ru.yandex.clickhouse.settings.ClickHouseProperties;

        import java.io.BufferedReader;
        import java.io.FileReader;
        import java.sql.*;
        import java.util.LinkedHashSet;
        import java.util.Set;

public class for_csv_mysql_to_ck {

    private static final int BATCH_SIZE = 3000; // 每批处理的数据量

    public static void main(String[] args) throws Exception {
        String csvFile = "D:\\wzza\\develop\\idea_project\\ceshi\\src\\main\\java\\mysqlutil\\a.xlsx";
        BufferedReader br = new BufferedReader(new FileReader(csvFile));
        String line;
        while ((line = br.readLine()) != null) {
            String[] parts = line.split(",");
            if (parts.length != 3) {
                System.err.println("Invalid line in CSV: " + line);
                continue;
            }

            String MYSQL_JDBC_URL = "jdbc:mysql://192.168.5.34:3306/" + parts[0].trim();
            String MYSQL_USER = "root";
            String MYSQL_PASSWORD = "smartpthdata";
            String CLICKHOUSE_JDBC_URL = "jdbc:clickhouse://hadoop110:8123/test";
            String CLICKHOUSE_USER = "default";
            String CLICKHOUSE_PASSWORD = "smartpath";
            String MYSQL_TABLE = parts[1].trim();
            String CLICKHOUSE_TABLE = parts[2].trim();

            System.out.println("Starting data migration from MySQL table " + MYSQL_TABLE +
                    " in database " + parts[0] + " to ClickHouse table " + CLICKHOUSE_TABLE + "...");

            Connection mysqlConn = DriverManager.getConnection(MYSQL_JDBC_URL, MYSQL_USER, MYSQL_PASSWORD);
            System.out.println("Connected to MySQL database.");

            ClickHouseProperties properties = new ClickHouseProperties();
            properties.setUser(CLICKHOUSE_USER);
            properties.setPassword(CLICKHOUSE_PASSWORD);
            ClickHouseDataSource clickHouseDataSource = new ClickHouseDataSource(CLICKHOUSE_JDBC_URL, properties);
            ClickHouseConnection clickhouseConn = clickHouseDataSource.getConnection();
            System.out.println("Connected to ClickHouse database.");

            // 数据迁移过程...
            migrateData(mysqlConn, clickhouseConn, MYSQL_TABLE, CLICKHOUSE_TABLE);

            // 关闭资源
            clickhouseConn.close();
            mysqlConn.close();
            System.out.println("Connection closed for table " + MYSQL_TABLE);
        }

        br.close();
        System.out.println("All data migration completed successfully.");
    }

    private static void migrateData(Connection mysqlConn, ClickHouseConnection clickhouseConn,
                                    String mysqlTableName, String clickhouseTableName) throws SQLException {
        // 获取 MySQL 表的列信息
        String sql = "SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?";
        PreparedStatement stmt = mysqlConn.prepareStatement(sql);
        stmt.setString(1, mysqlConn.getCatalog());
        stmt.setString(2, mysqlTableName);
        ResultSet rsColumns = stmt.executeQuery();

        Set<String> columns = new LinkedHashSet<>();
        StringBuilder createTableQuery = new StringBuilder("CREATE TABLE IF NOT EXISTS " + clickhouseTableName.toLowerCase() + " (");
        String primaryKeyColumn = null;

        // 根据 MySQL 表的列信息构建 ClickHouse 表的创建语句
        while (rsColumns.next()) {
            String columnName = rsColumns.getString("COLUMN_NAME").toLowerCase();
            String dataType = rsColumns.getString("DATA_TYPE").toLowerCase();
            int columnSize = rsColumns.getInt("CHARACTER_MAXIMUM_LENGTH");

            if (primaryKeyColumn == null) primaryKeyColumn = columnName;

            String clickhouseDataType = mapDataType(dataType, columnSize);

            if (columns.add(columnName)) {
                createTableQuery.append(columnName).append(" ").append(clickhouseDataType).append(", ");
            }
        }
        createTableQuery.setLength(createTableQuery.length() - 2); // Remove the last comma and space
        createTableQuery.append(") ENGINE = MergeTree() ORDER BY ").append(primaryKeyColumn);
        System.out.println("Creating ClickHouse table if not exists: " + createTableQuery);

        Statement clickhouseStmt = clickhouseConn.createStatement();
        try {
            clickhouseStmt.execute(createTableQuery.toString());
            System.out.println("ClickHouse table created successfully.");
        } catch (SQLException e) {
            System.err.println("Error creating ClickHouse table: " + e.getMessage());
            e.printStackTrace();
        }

        // 获取 ClickHouse 表的列信息
        ResultSet clickhouseColumns = clickhouseConn.createStatement().executeQuery("DESCRIBE TABLE " + clickhouseTableName.toLowerCase());
        Set<String> clickhouseColumnSet = new LinkedHashSet<>();
        while (clickhouseColumns.next()) {
            String columnName = clickhouseColumns.getString("name").toLowerCase();
            clickhouseColumnSet.add(columnName);
        }
        System.out.println("Retrieved ClickHouse table columns: " + clickhouseColumnSet);

        // 准备插入数据的 SQL 语句
        String insertColumns = String.join(", ", clickhouseColumnSet);
        String questionMarks = String.join(", ", clickhouseColumnSet.stream().map(c -> "?").toArray(String[]::new));
        String insertQuery = "INSERT INTO " + clickhouseTableName.toLowerCase() + " (" + insertColumns + ") VALUES (" + questionMarks + ")";
        System.out.println("Insert query: " + insertQuery);

        // 循环分片导入数据
        for (int i = 0; i < 10; i++) {
            String shardSelectQuery = "SELECT * FROM " + mysqlTableName + " WHERE MOD(ABS(CRC32(id)), 10) = " + i;
            PreparedStatement mysqlSelectStmt = mysqlConn.prepareStatement(shardSelectQuery);
            ResultSet mysqlDataRs = mysqlSelectStmt.executeQuery();

            PreparedStatement clickhouseInsertStmt = clickhouseConn.prepareStatement(insertQuery);

            int batchSize = 0;
            int totalInserted = 0;
            while (mysqlDataRs.next()) {
                int parameterIndex = 1;
                for (String columnName : clickhouseColumnSet) {
                    Object value = mysqlDataRs.getObject(columnName);
                    if (value instanceof String) {
                        value = ((String) value).replaceAll("[\\r\\n\\t]", "");
                    }
                    clickhouseInsertStmt.setObject(parameterIndex++, value);
                }
                clickhouseInsertStmt.addBatch();
                batchSize++;
                totalInserted++;

                if (batchSize % BATCH_SIZE == 0) {
                    clickhouseInsertStmt.executeBatch();
                    clickhouseInsertStmt.clearBatch();
                    System.out.println("Inserted batch of " + batchSize + " records for table " + mysqlTableName +
                            ", shard " + i + ", Total: " + totalInserted + " records.");
                    batchSize = 0;
                }
            }

            if (batchSize > 0) {
                clickhouseInsertStmt.executeBatch();
                System.out.println("Inserted remaining batch of " + batchSize + " records for table " + mysqlTableName +
                        ", shard " + i + ", Total: " + totalInserted + " records.");
            }

            mysqlDataRs.close();
            mysqlSelectStmt.close();
        }

        System.out.println("Data migration for table " + mysqlTableName + " completed successfully.");

        // 关闭资源
        clickhouseStmt.close();
    }

    private static String mapDataType(String mysqlDataType, int columnSize) {
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
