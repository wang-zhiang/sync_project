package sqlservertockutil.不经过linux同步;

import au.com.bytecode.opencsv.CSVReader;

import java.io.FileReader;
import java.io.IOException;
import java.sql.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

//sqlserver 读不进的话，可以改成分页查询  ，这样就能继续同步了
//注意修改ser_num的值

//评价是35和45号机一起的

/*
* --39号机赋值 121  35号机赋值35
* 数据库链接	192.168.99.35,2766
数据库表名	SearchCommentYHD
表名	dbo.trading2406

数据库链接	192.168.99.39,2800
数据库表名	WebSearch
表名	dbo.Trading2406
* */

public class spB2CWebsiteData_thread_new {
    // ser_num 配置：填数字表示写死的值，填"ser_num"表示使用SQL Server表中的ser_num字段值
    private static final String SER_NUM_CONFIG = "35"; // 可改为 "ser_num" 或具体数字如 "35"
    
    // SQL Server 连接信息
    //private static final String SQL_SERVER_URL = "jdbc:sqlserver://192.168.99.39:2800;databaseName=WebSearch";
     //private static final String SQL_SERVER_URL = "jdbc:sqlserver://192.168.99.35:2766;databaseName=SearchCommentYHD";
    private static final String SQL_SERVER_URL = "jdbc:sqlserver://192.168.99.45:2866;databaseName=searchcommentY264_31";
//    private static final String SQL_SERVER_USER = "sa";
//    private static final String SQL_SERVER_PASSWORD = "smartpthdata";
     private static final String SQL_SERVER_USER = "CHH";
    private static final String SQL_SERVER_PASSWORD = "Y1v606";
    // ClickHouse 连接信息
    private static final String CLICKHOUSE_URL = "jdbc:clickhouse://hadoop110:8123";
    private static final String CLICKHOUSE_USER = "default";
    private static final String CLICKHOUSE_PASSWORD = "smartpath";


    // ClickHouse 表名
    private static final String CK_TABLE_NAME = "dwd.spB2CWebsiteData";

    // 批处理大小
    private static final int BATCH_SIZE = 30000;

    public static void main(String[] args) {
        long startTime = System.currentTimeMillis(); // 记录开始时间
        System.out.println("程序开始时间: " + startTime);

        try {
            // 读取 CSV 文件，获取表名列表
            List<String> tableNames = readCsvFile("D:\\wzza\\develop\\idea_project\\ceshi\\src\\main\\java\\sqlservertockutil\\不经过linux同步\\b.csv");
            System.out.println("读取到的表名列表: " + tableNames);

            // 建立 SQL Server 和 ClickHouse 连接
            Connection sqlServerConn = DriverManager.getConnection(SQL_SERVER_URL, SQL_SERVER_USER, SQL_SERVER_PASSWORD);
            Connection clickHouseConn = DriverManager.getConnection(CLICKHOUSE_URL, CLICKHOUSE_USER, CLICKHOUSE_PASSWORD);
            System.out.println("成功连接到 SQL Server 和 ClickHouse");

            // 获取 ClickHouse 表的字段类型
            Map<String, String> ckFieldTypes = getClickHouseTableStructure(clickHouseConn);
            System.out.println("ClickHouse 表字段类型: " + ckFieldTypes);

            for (String tableName : tableNames) {
                System.out.println("开始处理表: " + tableName);
                // 使用线程池处理单个表数据同步
                ExecutorService executor = Executors.newFixedThreadPool(5); // 设置线程池大小为5

                // 从 SQL Server 分页获取表数据
                int offset = 0;
                while (true) {
                    List<Map<String, Object>> tableData = getSqlServerTableData(sqlServerConn, tableName, offset, BATCH_SIZE);
                    if (tableData.isEmpty()) {
                        break;
                    }

                    // 捕获当前的offset和batchData
                    int currentOffset = offset;
                    List<Map<String, Object>> batchData = tableData;
                    executor.submit(() -> {
                        try {
                            insertDataIntoClickHouse(clickHouseConn, batchData, ckFieldTypes, tableName);
                            System.out.println("插入数据到 ClickHouse 表，批次: " + (currentOffset / BATCH_SIZE + 1));
                        } catch (SQLException e) {
                            e.printStackTrace();
                        }
                    });

                    offset += BATCH_SIZE;
                }

                // 关闭当前表的线程池
                executor.shutdown();
                while (!executor.isTerminated()) {
                    // 等待所有任务完成
                    try {
                        executor.awaitTermination(1, TimeUnit.MINUTES);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
                System.out.println("表 " + tableName + " 同步完成");
            }

            // 关闭连接
            sqlServerConn.close();
            clickHouseConn.close();
            System.out.println("所有任务完成，连接已关闭");
        } catch (Exception e) {
            e.printStackTrace();
        }

        long endTime = System.currentTimeMillis(); // 记录结束时间
        long totalTimeMillis = endTime - startTime; // 计算总用时

        // 转换总用时为小时、分钟和秒
        long totalSeconds = totalTimeMillis / 1000;
        long hours = totalSeconds / 3600;
        long minutes = (totalSeconds % 3600) / 60;
        long seconds = totalSeconds % 60;

        System.out.println(String.format("总耗时: %d小时 %d分钟 %d秒", hours, minutes, seconds));
    }

    // 读取 CSV 文件
    private static List<String> readCsvFile(String filePath) throws IOException {
        List<String> tableNames = new ArrayList<>();
        try (CSVReader reader = new CSVReader(new FileReader(filePath))) {
            String[] line;
            while ((line = reader.readNext()) != null) {
                tableNames.add(line[0]);
            }
        }
        System.out.println("成功读取 CSV 文件，获取到表名列表: " + tableNames);
        return tableNames;
    }

    // 获取 ClickHouse 表结构
    private static Map<String, String> getClickHouseTableStructure(Connection ckConn) throws SQLException {
        Map<String, String> fieldTypes = new HashMap<>();
        String query = "DESCRIBE TABLE " + CK_TABLE_NAME;
        try (Statement stmt = ckConn.createStatement(); ResultSet rs = stmt.executeQuery(query)) {
            while (rs.next()) {
                String fieldName = rs.getString("name");
                // 跳过 ser_num 和 pt_ym 字段
                if (!fieldName.equalsIgnoreCase("ser_num") && !fieldName.equalsIgnoreCase("pt_ym")) {
                    fieldTypes.put(fieldName, rs.getString("type"));
                }
            }
        }
        System.out.println("获取到 ClickHouse 表结构: " + fieldTypes);
        return fieldTypes;
    }

    // 从 SQL Server 分页获取表数据
    private static List<Map<String, Object>> getSqlServerTableData(Connection sqlServerConn, String tableName, int offset, int limit) throws SQLException {
        List<Map<String, Object>> tableData = new ArrayList<>();
        // 根据配置决定是否需要排除ser_num字段（如果使用写死值，则不需要查询ser_num字段）
        String selectFields = "*";
        if (!"ser_num".equals(SER_NUM_CONFIG)) {
            // 如果不使用表中的ser_num，可以排除该字段以避免冲突（可选）
            selectFields = "*";
        }
        String query = "SELECT " + selectFields + " FROM (SELECT *, ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS rn FROM " + tableName + ") AS temp WHERE rn BETWEEN ? AND ?";
        try (PreparedStatement stmt = sqlServerConn.prepareStatement(query)) {
            stmt.setInt(1, offset + 1);
            stmt.setInt(2, offset + limit);
            try (ResultSet rs = stmt.executeQuery()) {
                tableData = ResultSetConverter.convertResultSetToList(rs);
            }
        }
        System.out.println("获取到 SQL Server 表数据: " + tableName + ", 大小: " + tableData.size());
        return tableData;
    }

    // 将数据插入到 ClickHouse 表中
    private static void insertDataIntoClickHouse(Connection clickHouseConn, List<Map<String, Object>> tableData, Map<String, String> ckFieldTypes, String sqlServerTableName) throws SQLException {
        String ptYmValue = "20" + sqlServerTableName.substring(sqlServerTableName.length() - 4);

        // 构建 ClickHouse 插入语句
        StringBuilder insertQuery = new StringBuilder("INSERT INTO " + CK_TABLE_NAME + " (ser_num, pt_ym");
        for (String ckField : ckFieldTypes.keySet()) {
            insertQuery.append(", ").append(ckField);
        }
        insertQuery.append(") VALUES (?, ?, ");
        for (int j = 0; j < ckFieldTypes.size(); j++) {
            insertQuery.append("?, ");
        }
        insertQuery.setLength(insertQuery.length() - 2); // 去掉最后的逗号
        insertQuery.append(")");

        try (PreparedStatement ckStmt = clickHouseConn.prepareStatement(insertQuery.toString())) {
            for (int rowIndex = 0; rowIndex < tableData.size(); rowIndex++) {
                Map<String, Object> row = tableData.get(rowIndex);
                
                // 根据配置设置ser_num值
                String serNumValue;
                if ("ser_num".equals(SER_NUM_CONFIG)) {
                    // 使用SQL Server表中的ser_num字段值
                    Object serNumObj = row.get("ser_num");
                    serNumValue = serNumObj != null ? serNumObj.toString() : "0";
                } else {
                    // 使用配置的写死值
                    serNumValue = SER_NUM_CONFIG;
                }
                ckStmt.setString(1, serNumValue); // ser_num
                ckStmt.setString(2, ptYmValue); // pt_ym

                int columnIndex = 3;
                for (String ckField : ckFieldTypes.keySet()) {
                    Object sqlValue = row.get(ckField.toLowerCase());
                    if (sqlValue != null) {
                        setPreparedStatementValue(ckStmt, columnIndex, sqlValue.toString(), ckFieldTypes.get(ckField));
                    } else {
                        setPreparedStatementValue(ckStmt, columnIndex, null, ckFieldTypes.get(ckField));
                    }
                    columnIndex++;
                }
                ckStmt.addBatch();

                if ((rowIndex + 1) % BATCH_SIZE == 0) {
                    ckStmt.executeBatch();
                    System.out.println("执行批量插入，当前批次大小: " + BATCH_SIZE);
                }
            }

            // 执行剩余的批处理
            if (tableData.size() % BATCH_SIZE != 0) {
                ckStmt.executeBatch();
                System.out.println("执行剩余批处理，大小: " + tableData.size() % BATCH_SIZE);
            }
        }
    }

    // 设置 PreparedStatement 的值
    private static void setPreparedStatementValue(PreparedStatement stmt, int index, String value, String dataType) throws SQLException {
        try {
            if (value == null) {
                switch (dataType.toLowerCase()) {
                    case "int32":
                        stmt.setInt(index, 0);
                        break;
                    case "int64":
                        stmt.setLong(index, 0L);
                        break;
                    case "float32":
                        stmt.setFloat(index, 0.0f);
                        break;
                    case "float64":
                        stmt.setDouble(index, 0.0);
                        break;
                    case "boolean":
                        stmt.setBoolean(index, false);
                        break;
                    case "datetime":
                        stmt.setTimestamp(index, Timestamp.valueOf("1970-01-01 00:00:00"));
                        break;
                    case "string":
                    default:
                        stmt.setString(index, "");
                        break;
                }
            } else {
                switch (dataType.toLowerCase()) {
                    case "int32":
                        stmt.setInt(index, Integer.parseInt(value));
                        break;
                    case "int64":
                        stmt.setLong(index, Long.parseLong(value));
                        break;
                    case "float32":
                        stmt.setFloat(index, Float.parseFloat(value));
                        break;
                    case "float64":
                        stmt.setDouble(index, Double.parseDouble(value));
                        break;
                    case "boolean":
                        stmt.setBoolean(index, Boolean.parseBoolean(value));
                        break;
                    case "datetime":
                        stmt.setTimestamp(index, convertToTimestamp(value));
                        break;
                    case "string":
                    default:
                        stmt.setString(index, value);
                        break;
                }
            }
        } catch (NumberFormatException | ParseException e) {
            setPreparedStatementValue(stmt, index, null, dataType);
        }
    }

    // 转换字符串到 Timestamp
    private static Timestamp convertToTimestamp(String value) throws ParseException {
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        return new Timestamp(dateFormat.parse(value.substring(0, 19)).getTime());
    }

    // ResultSetConverter 类定义
    static class ResultSetConverter {
        public static List<Map<String, Object>> convertResultSetToList(ResultSet rs) throws SQLException {
            List<Map<String, Object>> resultList = new ArrayList<>();
            ResultSetMetaData md = rs.getMetaData();
            int columns = md.getColumnCount();
            while (rs.next()) {
                Map<String, Object> row = new HashMap<>(columns);
                for (int i = 1; i <= columns; ++i) {
                    row.put(md.getColumnName(i).toLowerCase(), rs.getObject(i));
                }
                resultList.add(row);
            }
            return resultList;
        }
    }
}
