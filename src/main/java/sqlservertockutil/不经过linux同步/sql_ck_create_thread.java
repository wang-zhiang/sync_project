package sqlservertockutil.不经过linux同步;



import java.sql.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class sql_ck_create_thread {
    // SQL Server连接详情
    private static final String SQL_SERVER_URL = "jdbc:sqlserver://192.168.99.31:2722;DatabaseName=SearchCommentY264";
    private static final String SQL_SERVER_USER = "sa";
    private static final String SQL_SERVER_PASSWORD = "smartpthdata";
    private static final String SQL_SERVER_QUERY_TEMPLATE = "SELECT * FROM trading2405原始 WHERE ABS(CHECKSUM(Id)) % 10 = ?"; // 分片查询模板

    // ClickHouse连接详情
    private static final String CLICKHOUSE_URL = "jdbc:clickhouse://hadoop110:8123/wph";
    private static final String CLICKHOUSE_USER = "default";
    private static final String CLICKHOUSE_PASSWORD = "smartpath";
    private static final String CLICKHOUSE_TABLE = "wph.tradingOriginal264_2404";

    // 插入的批处理大小
    private static final int BATCH_SIZE = 30000;

    public static void main(String[] args) {
        // 创建一个包含5个线程的线程池
        ExecutorService executorService = Executors.newFixedThreadPool(5);

        // 循环10次，提交10个分片的插入任务给线程池
        for (int i = 0; i < 10; i++) {
            int partition = i; // 分片编号
            executorService.submit(() -> insertPartition(partition)); // 提交任务
        }

        // 关闭线程池
        executorService.shutdown();
    }

    // 插入指定分片的数据
    private static void insertPartition(int partition) {
        Connection sqlServerConnection = null;
        Connection clickHouseConnection = null;
        PreparedStatement sqlServerPreparedStatement = null;
        PreparedStatement clickHousePreparedStatement = null;
        ResultSet resultSet = null;

        try {
            // 第一步：连接到SQL Server
            sqlServerConnection = DriverManager.getConnection(SQL_SERVER_URL, SQL_SERVER_USER, SQL_SERVER_PASSWORD);

            // 获取列的元数据
            Statement sqlServerStatement = sqlServerConnection.createStatement();
            resultSet = sqlServerStatement.executeQuery("SELECT * FROM trading2405原始 WHERE 1 = 0"); // 获取列信息
            ResultSetMetaData metaData = resultSet.getMetaData();
            int columnCount = metaData.getColumnCount();
            System.out.println("Column count: " + columnCount);
            for (int i = 1; i <= columnCount; i++) {
                System.out.println("Column " + i + ": " + metaData.getColumnName(i) + " (" + metaData.getColumnTypeName(i) + ")");
            }
            StringBuilder columns = new StringBuilder();
            StringBuilder placeholders = new StringBuilder();
            StringBuilder createTableQuery = new StringBuilder();
            String firstColumn = null;

            // 构建ClickHouse表的创建查询
            createTableQuery.append("CREATE TABLE IF NOT EXISTS ").append(CLICKHOUSE_TABLE).append(" (");

            for (int i = 1; i <= columnCount; i++) {
                String columnName = metaData.getColumnName(i);
                String pinyinColumnName = chinesetopinyin.chineseToPinyin(columnName); // 调用接口转换字段名
                String sqlType = metaData.getColumnTypeName(i);
                String clickHouseType = mapSqlTypeToClickHouseType(sqlType);

                if (i == 1) {
                    firstColumn = pinyinColumnName; // 设置第一个列名用于ORDER BY
                }

                columns.append(pinyinColumnName); // 构建列名字符串
                placeholders.append("?"); // 构建占位符字符串
                createTableQuery.append(pinyinColumnName).append(" ").append(clickHouseType); // 构建创建表的SQL语句

                if (i < columnCount) {
                    columns.append(", ");
                    placeholders.append(", ");
                    createTableQuery.append(", ");
                }
            }

            createTableQuery.append(") ENGINE = MergeTree() ORDER BY ").append(firstColumn).append(";");

            // 第二步：连接到ClickHouse
            clickHouseConnection = DriverManager.getConnection(CLICKHOUSE_URL, CLICKHOUSE_USER, CLICKHOUSE_PASSWORD);
            Statement clickHouseStatement = clickHouseConnection.createStatement();

            // 如果ClickHouse表不存在，则创建它
            try {
                clickHouseStatement.execute(createTableQuery.toString());
                System.out.println("Table created successfully in ClickHouse.");
            } catch (SQLException e) {
                System.err.println("Error creating table in ClickHouse: " + e.getMessage());
                return;
            }

            // 准备ClickHouse的插入查询
            String clickHouseInsertQuery = String.format("INSERT INTO %s (%s) VALUES (%s)", CLICKHOUSE_TABLE, columns, placeholders);
            clickHousePreparedStatement = clickHouseConnection.prepareStatement(clickHouseInsertQuery);

            // 第三步：处理来自SQL Server的每一行，并插入到ClickHouse中
            boolean success = false;
            while (!success) {
                try {
                    sqlServerPreparedStatement = sqlServerConnection.prepareStatement(SQL_SERVER_QUERY_TEMPLATE);
                    sqlServerPreparedStatement.setInt(1, partition); // 设置分片编号
                    resultSet = sqlServerPreparedStatement.executeQuery();

                    int count = 0;
                    while (resultSet.next()) {
                        for (int j = 1; j <= columnCount; j++) {
                            String value = resultSet.getString(j);
                            if (value != null) {
                                // 处理ClickHouse的DateTime格式
                                if (metaData.getColumnTypeName(j).equalsIgnoreCase("DATETIME") || metaData.getColumnTypeName(j).equalsIgnoreCase("TIMESTAMP")) {
                                    value = value.substring(0, 19); // 截断为"YYYY-MM-DD hh:mm:ss"
                                }
                                value = value.replace("\n", "").replace("\t", "").replace("\r", ""); // 移除换行符、制表符和回车符
                            }
                            clickHousePreparedStatement.setString(j, value); // 设置插入值
                        }
                        clickHousePreparedStatement.addBatch(); // 添加到批处理中
                        count++;

                        if (count % BATCH_SIZE == 0) {
                            clickHousePreparedStatement.executeBatch(); // 执行批处理
                            System.out.println("Inserted " + count + " rows into ClickHouse for partition " + partition + ".");
                        }
                    }

                    // 执行剩余的批处理
                    if (count % BATCH_SIZE != 0) {
                        clickHousePreparedStatement.executeBatch();
                        System.out.println("Inserted " + count + " rows into ClickHouse for partition " + partition + ".");
                    }
                    success = true; // 标记成功
                } catch (SQLException e) {
                    System.err.println("Error inserting into ClickHouse for partition " + partition + ": " + e.getMessage());
                    System.err.println("Retrying partition " + partition + "...");
                    // 可选：在重试前添加延迟
                    try {
                        Thread.sleep(5000); // 等待5秒钟后重试
                    } catch (InterruptedException ie) {
                        ie.printStackTrace();
                    }
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            // 关闭资源
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

    // 将SQL Server数据类型映射到ClickHouse数据类型
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
                return "String"; // 如果类型未知，默认为String
        }
    }
}
