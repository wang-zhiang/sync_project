package mysqlutil;

import java.io.*;
import java.sql.*;
import java.util.*;

/**
 * CSV文件同步到MySQL工具类
 * 功能：
 * 1. 读取CSV文件（第一行作为字段名）
 * 2. 自动检查MySQL中是否存在对应表
 * 3. 如果不存在则自动创建表
 * 4. 将CSV数据插入到MySQL表中
 */
public class CsvToMySQLSync {
    
    // MySQL数据库连接配置
    private static final String MYSQL_HOST = "192.168.6.101";
    private static final String MYSQL_PORT = "3306";
    private static final String MYSQL_DATABASE = "mdlz";
    private static final String MYSQL_USER = "root";
    private static final String MYSQL_PASSWORD = "smartpath";
    private static final String MYSQL_URL = "jdbc:mysql://" + MYSQL_HOST + ":" + MYSQL_PORT + "/" + MYSQL_DATABASE 
        + "?useSSL=false&serverTimezone=UTC&allowPublicKeyRetrieval=true&rewriteBatchedStatements=true"
        + "&useUnicode=true&characterEncoding=utf8";
    
    // 批量插入大小
    private static final int BATCH_SIZE = 5000;
    
    public static void main(String[] args) {
        // 示例用法
        String csvFilePath = "D:\\wzza\\develop\\idea_project\\ceshi\\src\\main\\java\\mysqlutil\\csv\\20250718 LSL cake & Biscuit Data (2301-2505).csv";
        String tableName = "mdlz_cake_biscuit";
        
        try {
            syncCsvToMySQL(csvFilePath, tableName);
        } catch (Exception e) {
            System.err.println("同步失败: " + e.getMessage());
            e.printStackTrace();
        }
    }
    
    /**
     * 将CSV文件同步到MySQL表
     * @param csvFilePath CSV文件路径
     * @param tableName 目标表名
     * @throws Exception 异常
     */
    public static void syncCsvToMySQL(String csvFilePath, String tableName) throws Exception {
        System.out.println("开始同步CSV文件到MySQL...");
        System.out.println("CSV文件路径: " + csvFilePath);
        System.out.println("目标表名: " + tableName);
        
        Connection connection = null;
        BufferedReader reader = null;
        
        try {
            // 连接MySQL数据库
            connection = DriverManager.getConnection(MYSQL_URL, MYSQL_USER, MYSQL_PASSWORD);
            System.out.println("成功连接到MySQL数据库");
            
            // 读取CSV文件
            reader = new BufferedReader(new FileReader(csvFilePath));
            String headerLine = reader.readLine();
            
            if (headerLine == null || headerLine.trim().isEmpty()) {
                throw new Exception("CSV文件为空或第一行为空");
            }
            
            // 解析CSV头部（字段名）
            String[] columns = parseCSVLine(headerLine);
            System.out.println("检测到字段: " + Arrays.toString(columns));
            
            // 检查表是否存在，不存在则创建
            if (!tableExists(connection, tableName)) {
                System.out.println("表 " + tableName + " 不存在，正在创建...");
                createTable(connection, tableName, columns);
                System.out.println("表 " + tableName + " 创建成功");
            } else {
                System.out.println("表 " + tableName + " 已存在");
            }
            
            // 获取实际的表结构
            List<String> actualColumns = getTableColumns(connection, tableName);
            System.out.println("表实际字段: " + actualColumns);
            
            // 验证CSV字段与表字段的匹配
            validateColumnMapping(columns, actualColumns);
            
            // 插入数据
            insertData(connection, tableName, reader, columns);
            
            System.out.println("CSV文件同步完成！");
            
        } finally {
            if (reader != null) {
                try {
                    reader.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            if (connection != null) {
                try {
                    connection.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }
    
    /**
     * 解析CSV行，处理逗号分隔和引号包围的情况，并过滤空字段
     * @param line CSV行
     * @return 解析后的字段数组（过滤掉空字段）
     */
    private static String[] parseCSVLine(String line) {
        List<String> result = new ArrayList<>();
        StringBuilder current = new StringBuilder();
        boolean inQuotes = false;
        
        for (int i = 0; i < line.length(); i++) {
            char c = line.charAt(i);
            
            if (c == '"') {
                inQuotes = !inQuotes;
            } else if (c == ',' && !inQuotes) {
                String field = current.toString().trim();
                if (!field.isEmpty()) { // 只添加非空字段
                    result.add(field);
                }
                current = new StringBuilder();
            } else {
                current.append(c);
            }
        }
        
        // 处理最后一个字段
        String lastField = current.toString().trim();
        if (!lastField.isEmpty()) {
            result.add(lastField);
        }
        
        return result.toArray(new String[0]);
    }
    
    /**
     * 检查表是否存在
     * @param connection 数据库连接
     * @param tableName 表名
     * @return 是否存在
     * @throws SQLException SQL异常
     */
    private static boolean tableExists(Connection connection, String tableName) throws SQLException {
        DatabaseMetaData metaData = connection.getMetaData();
        ResultSet resultSet = metaData.getTables(null, null, tableName, null);
        return resultSet.next();
    }
    
    /**
     * 创建表
     * @param connection 数据库连接
     * @param tableName 表名
     * @param columns 字段名数组
     * @throws SQLException SQL异常
     */
    private static void createTable(Connection connection, String tableName, String[] columns) throws SQLException {
        StringBuilder createTableSQL = new StringBuilder("CREATE TABLE `" + tableName + "` (");
        
        for (int i = 0; i < columns.length; i++) {
            String columnName = columns[i].replaceAll("[^a-zA-Z0-9_]", "_"); // 清理字段名
            createTableSQL.append("`").append(columnName).append("` TEXT");
            if (i < columns.length - 1) {
                createTableSQL.append(", ");
            }
        }
        
        createTableSQL.append(") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4");
        
        Statement statement = connection.createStatement();
        statement.execute(createTableSQL.toString());
        statement.close();
    }
    
    /**
     * 获取表的字段列表
     * @param connection 数据库连接
     * @param tableName 表名
     * @return 字段列表
     * @throws SQLException SQL异常
     */
    private static List<String> getTableColumns(Connection connection, String tableName) throws SQLException {
        List<String> columns = new ArrayList<>();
        DatabaseMetaData metaData = connection.getMetaData();
        ResultSet resultSet = metaData.getColumns(null, null, tableName, null);
        
        while (resultSet.next()) {
            columns.add(resultSet.getString("COLUMN_NAME"));
        }
        
        return columns;
    }
    
    /**
     * 验证CSV字段与表字段的匹配
     * @param csvColumns CSV字段
     * @param tableColumns 表字段
     * @throws Exception 异常
     */
    private static void validateColumnMapping(String[] csvColumns, List<String> tableColumns) throws Exception {
        for (String csvColumn : csvColumns) {
            // 跳过空字段（这些字段在parseCSVLine中已经被过滤掉了）
            if (csvColumn == null || csvColumn.trim().isEmpty()) {
                continue;
            }
            
            String cleanColumnName = csvColumn.replaceAll("[^a-zA-Z0-9_]", "_");
            if (!tableColumns.contains(cleanColumnName)) {
                throw new Exception("CSV字段 '" + csvColumn + "' 在表中不存在，清理后的字段名: '" + cleanColumnName + "'");
            }
        }
        System.out.println("字段映射验证通过");
    }
    
    /**
     * 插入数据
     * @param connection 数据库连接
     * @param tableName 表名
     * @param reader CSV读取器
     * @param columns 字段数组
     * @throws Exception 异常
     */
    private static void insertData(Connection connection, String tableName, BufferedReader reader, String[] columns) throws Exception {
        // 构建插入SQL
        StringBuilder insertSQL = new StringBuilder("INSERT INTO `" + tableName + "` (");
        
        for (int i = 0; i < columns.length; i++) {
            String columnName = columns[i].replaceAll("[^a-zA-Z0-9_]", "_");
            insertSQL.append("`").append(columnName).append("`");
            if (i < columns.length - 1) {
                insertSQL.append(", ");
            }
        }
        
        insertSQL.append(") VALUES (");
        for (int i = 0; i < columns.length; i++) {
            insertSQL.append("?");
            if (i < columns.length - 1) {
                insertSQL.append(", ");
            }
        }
        insertSQL.append(")");
        
        PreparedStatement preparedStatement = connection.prepareStatement(insertSQL.toString());
        
        String line;
        int batchCount = 0;
        int totalCount = 0;
        
        System.out.println("开始插入数据...");
        
        while ((line = reader.readLine()) != null) {
            if (line.trim().isEmpty()) {
                continue;
            }
            
            String[] values = parseCSVLine(line);
            
            if (values.length != columns.length) {
                System.err.println("跳过字段数量不匹配的行 (期望: " + columns.length + ", 实际: " + values.length + "): " + line);
                continue;
            }
            
            for (int i = 0; i < values.length; i++) {
                preparedStatement.setString(i + 1, values[i]);
            }
            
            preparedStatement.addBatch();
            batchCount++;
            totalCount++;
            
            if (batchCount >= BATCH_SIZE) {
                preparedStatement.executeBatch();
                preparedStatement.clearBatch();
                batchCount = 0;
                System.out.println("已插入 " + totalCount + " 条记录");
            }
        }
        
        // 插入剩余的数据
        if (batchCount > 0) {
            preparedStatement.executeBatch();
        }
        
        preparedStatement.close();
        System.out.println("数据插入完成，总共插入了 " + totalCount + " 条记录");
    }
} 