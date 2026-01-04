package sqlservertockutil.不经过linux同步;

import au.com.bytecode.opencsv.CSVReader;
import org.apache.poi.ss.usermodel.*;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStreamReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

//test_0724,test_0724_new （csv数据格式）  可做测试，时区


public class from_csv_to_ck_check {
    // SQL Server连接详情
   // private static final String SQL_SERVER_URL = "jdbc:sqlserver://192.168.4.201:2422;DatabaseName=taobao_trading";
   // private static final String SQL_SERVER_URL = "jdbc:sqlserver://192.168.4.35;DatabaseName=taobao_trading";
    //private static final String SQL_SERVER_URL = "jdbc:sqlserver://192.168.4.212:2533;DatabaseName=websearchc";
    // private static final String SQL_SERVER_URL = "jdbc:sqlserver://192.168.4.51;DatabaseName=taobao_trading";
    //private static final String SQL_SERVER_URL = "jdbc:sqlserver://192.168.3.182;DatabaseName=SyncWebSearchJD";
   // private static final String SQL_SERVER_URL = "jdbc:sqlserver://192.168.3.183;DatabaseName=sourcedate";
      private static final String SQL_SERVER_URL = "jdbc:sqlserver://192.168.4.36;DatabaseName=websearchc";
    //private static final String SQL_SERVER_URL = "jdbc:sqlserver://192.168.4.38;DatabaseName=trading_medicine";
    // private static final String SQL_SERVER_URL = "jdbc:sqlserver://smartpath10.tpddns.cn:2988;DatabaseName=TradingDouYin";
     // private static final String SQL_SERVER_URL =  "jdbc:sqlserver://192.168.4.57;DatabaseName=TradingDouYin";
   // private static final String SQL_SERVER_URL = "jdbc:sqlserver://192.168.4.57;DatabaseName=WebSearchPinduoduo";
    // private static final String SQL_SERVER_URL = "jdbc:sqlserver://192.168.3.181;DatabaseName=SyncTmallShop";
   // private static final String SQL_SERVER_URL = "jdbc:sqlserver://192.168.3.72;DatabaseName=TradingDouYin1111";
    // private static final String SQL_SERVER_URL = "jdbc:sqlserver://192.168.99.39:2800;DatabaseName=WebSearch";   //陈列
    // private static final String SQL_SERVER_URL = "jdbc:sqlserver://192.168.99.35:2766;DatabaseName=SearchCommentYHD";  //评价
    // private static final String SQL_SERVER_URL = "jdbc:sqlserver://192.168.99.45:2866;DatabaseName=searchcommentY264_31"; // 评价



    private static final String SQL_SERVER_USER = "CHH";
    private static final String SQL_SERVER_PASSWORD = "Y1v606";
//    private static final String SQL_SERVER_USER = "sa";
//    private static final String SQL_SERVER_PASSWORD = "smartpthdata";
//   private static final String SQL_SERVER_USER = "ldd";
//    private static final String SQL_SERVER_PASSWORD = "W1t459";
//   private static final String SQL_SERVER_USER = "sa";
//    private static final String SQL_SERVER_PASSWORD = "smartpathdata";
    // ClickHouse连接详情
    //private static final String CLICKHOUSE_URL = "jdbc:clickhouse://hadoop110:8123/ods";
    private static final String CLICKHOUSE_URL = "jdbc:clickhouse://192.168.5.111:8123/";
    private static final String CLICKHOUSE_USER = "default";
    private static final String CLICKHOUSE_PASSWORD = "smartpath";
    private static final String CLICKHOUSE_DATABASE = "ods";

    // 插入的批处理大小
    private static final int BATCH_SIZE = 30000;

    // SQL Server查询模板
    private static final String SQL_SERVER_QUERY_TEMPLATE = "SELECT * FROM (SELECT *, ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS RowNum FROM %s) AS TempTable WHERE RowNum %% 10 = ?"; // 分片查询模板

    public static void main(String[] args) {
        long startTime = System.currentTimeMillis(); // 记录开始时间

        Map<String, String> tableMappings = readCSV("D:\\wzza\\develop\\idea_project\\ceshi\\src\\main\\java\\sqlservertockutil\\不经过linux同步\\a.csv");

        // 创建一个包含5个线程的线程池
        ExecutorService executorService = Executors.newFixedThreadPool(10);

        // 循环处理每个表的分片
        for (Map.Entry<String, String> entry : tableMappings.entrySet()) {
            String sqlServerTableName = entry.getKey().trim();
            String clickHouseTableName = entry.getValue().trim();
            String fullClickHouseTableName = CLICKHOUSE_DATABASE + "." + clickHouseTableName;

            for (int i = 0; i < 10; i++) {
                int partition = i; // 分片编号
                executorService.submit(() -> insertPartition(sqlServerTableName, fullClickHouseTableName, partition)); // 提交任务
            }
        }

        // 关闭线程池并等待所有任务完成
        executorService.shutdown();
        try {
            if (executorService.awaitTermination(1, TimeUnit.HOURS)) {
                for (Map.Entry<String, String> entry : tableMappings.entrySet()) {
                    String sqlServerTableName = entry.getKey().trim();
                    String clickHouseTableName = entry.getValue().trim();
                    writeDataCountsToExcel(sqlServerTableName, clickHouseTableName);
                }
            } else {
                System.err.println("Tasks did not finish in the expected time.");
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }
        long endTime = System.currentTimeMillis(); // 记录结束时间
        long totalTime = endTime - startTime; // 计算总用时
        // 转换总用时为小时、分钟和秒
        long totalSeconds = totalTime / 1000;
        long hours = totalSeconds / 3600;
        long minutes = (totalSeconds % 3600) / 60;
        long seconds = totalSeconds % 60;

        System.out.println(String.format("总耗时: %d小时 %d分钟 %d秒", hours, minutes, seconds));
    }

    // 插入指定分片的数据
    private static void insertPartition(String sqlServerTableName, String clickHouseTableName, int partition) {
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
            resultSet = sqlServerStatement.executeQuery("SELECT * FROM " + sqlServerTableName + " WHERE 1 = 0"); // 获取列信息
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
            createTableQuery.append("CREATE TABLE IF NOT EXISTS ").append(clickHouseTableName).append(" (");

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
                System.out.println("Creating table with query: " + createTableQuery.toString());
                clickHouseStatement.execute(createTableQuery.toString());
                System.out.println("Table created successfully in ClickHouse.");
            } catch (SQLException e) {
                System.err.println("Error creating table in ClickHouse: " + e.getMessage());
                System.err.println("CreateTableQuery: " + createTableQuery.toString());
                return;
            }

            // 准备ClickHouse的插入查询
            System.out.println(clickHouseTableName);
            String clickHouseInsertQuery = String.format("INSERT INTO %s (%s) VALUES (%s)", clickHouseTableName, columns, placeholders);
            clickHousePreparedStatement = clickHouseConnection.prepareStatement(clickHouseInsertQuery);

            // 第三步：处理来自SQL Server的每一行，并插入到ClickHouse中
            boolean success = false;
            while (!success) {
                try {
                    sqlServerPreparedStatement = sqlServerConnection.prepareStatement(String.format(SQL_SERVER_QUERY_TEMPLATE, sqlServerTableName));
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
                return "Float64";
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

    private static Map<String, String> readCSV(String filePath) {
        Map<String, String> tableMappings = new HashMap<>();
        try (CSVReader reader = new CSVReader(new InputStreamReader(new FileInputStream(filePath), StandardCharsets.UTF_8), ',')) {
            String[] line;
            while ((line = reader.readNext()) != null) {
                if (line.length < 2) continue; // 确保每行至少有两个值
                String sqlServerTableName = line[0].trim();
                String clickHouseTableName = line[1].trim();
                System.out.println("SQL Server表名：" + sqlServerTableName);
                System.out.println("ClickHouse表名：" + clickHouseTableName);
                tableMappings.put(sqlServerTableName, clickHouseTableName);
            }
        } catch (IOException e) {
            System.err.println("Error reading CSV file");
            e.printStackTrace();
        }
        return tableMappings;
    }

    // 获取表的记录数
    private static int getTableRowCount(Connection connection, String query) throws SQLException {
        try (Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery(query)) {
            resultSet.next();
            return resultSet.getInt(1);
        }
    }

    // 将数据量写入Excel文件
    private static void writeDataCountsToExcel(String sqlServerTableName, String clickHouseTableName) throws Exception {
        String fullClickHouseTableName = CLICKHOUSE_DATABASE + "." + clickHouseTableName;
        // 获取SQL Server表的数据量
        int sqlServerRowCount;
        try (Connection sqlServerConnection = DriverManager.getConnection(SQL_SERVER_URL, SQL_SERVER_USER, SQL_SERVER_PASSWORD)) {
            String sqlServerCountQuery = "SELECT COUNT(*) FROM " + sqlServerTableName;
            sqlServerRowCount = getTableRowCount(sqlServerConnection, sqlServerCountQuery);
        }

        // 获取ClickHouse表的数据量
        int clickHouseRowCount;
        try (Connection clickHouseConnection = DriverManager.getConnection(CLICKHOUSE_URL, CLICKHOUSE_USER, CLICKHOUSE_PASSWORD)) {
            String clickHouseCountQuery = "SELECT COUNT(*) FROM " + fullClickHouseTableName;
            clickHouseRowCount = getTableRowCount(clickHouseConnection, clickHouseCountQuery);
        }

        // 写入Excel文件
        String excelFilePath = "data_counts.xlsx";
        Workbook workbook;
        Sheet sheet;
        boolean isNewFile = false;
        try (FileInputStream fis = new FileInputStream(excelFilePath)) {
            workbook = new XSSFWorkbook(fis);
            sheet = workbook.getSheetAt(0);
        } catch (IOException e) {
            workbook = new XSSFWorkbook();
            sheet = workbook.createSheet("DataCounts");
            isNewFile = true;
        }

        if (isNewFile) {
            Row headerRow = sheet.createRow(0);
            headerRow.createCell(0).setCellValue("SQL Server Table Name");
            headerRow.createCell(1).setCellValue("ClickHouse Table Name");
            headerRow.createCell(2).setCellValue("SQL Server Row Count");
            headerRow.createCell(3).setCellValue("ClickHouse Row Count");
            headerRow.createCell(4).setCellValue("Equal");
        }

        int lastRowNum = sheet.getLastRowNum();
        Row row = sheet.createRow(lastRowNum + 1);

        Cell sqlServerTableCell = row.createCell(0);
        sqlServerTableCell.setCellValue(sqlServerTableName);

        Cell clickHouseTableCell = row.createCell(1);
        clickHouseTableCell.setCellValue(CLICKHOUSE_DATABASE + "." + clickHouseTableName);

        Cell sqlServerRowCountCell = row.createCell(2);
        sqlServerRowCountCell.setCellValue(sqlServerRowCount);

        Cell clickHouseRowCountCell = row.createCell(3);
        clickHouseRowCountCell.setCellValue(clickHouseRowCount);

        Cell equalCell = row.createCell(4);
        equalCell.setCellValue(sqlServerRowCount == clickHouseRowCount);

        try (FileOutputStream fos = new FileOutputStream(excelFilePath)) {
            workbook.write(fos);
        }

        workbook.close();
    }
}
