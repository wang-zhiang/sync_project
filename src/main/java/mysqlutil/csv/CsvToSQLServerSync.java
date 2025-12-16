package mysqlutil;

import java.io.*;
import java.sql.*;
import java.util.*;

/**
 * CSV文件同步到SQL Server工具类
 * 功能：
 * 1. 读取CSV文件（第一行作为字段名）
 * 2. 自动检查SQL Server中是否存在对应表
 * 3. 如果不存在则自动创建表（所有字段类型为NVARCHAR(MAX)）
  * 4. 将CSV数据插入到SQL Server表中
 */
public class CsvToSQLServerSync {
    
    // SQL Server数据库连接配置
    private static final String SQLSERVER_HOST = "192.168.4.39";
    private static final String SQLSERVER_DATABASE = "o2o";
    private static final String SQLSERVER_USER = "sa";
    private static final String SQLSERVER_PASSWORD = "smartpthdata";
    private static final String SQLSERVER_URL = "jdbc:sqlserver://" + SQLSERVER_HOST + ";databaseName=" + SQLSERVER_DATABASE
        + ";trustServerCertificate=true;connectTimeout=30000;socketTimeout=1200000;loginTimeout=30000"
        + ";lockTimeout=600000;queryTimeout=600";
    
    // 批量插入大小
    private static final int BATCH_SIZE = 10000;
    
    public static void main(String[] args) {
        // 测试CSV解析
        if (args.length > 0 && "test".equals(args[0])) {
            testCSVParsing();
            return;
        }
        
        // 示例用法
        String csvFilePath = "D:\\wzza\\develop\\idea_project\\ceshi\\src\\main\\java\\mysqlutil\\csv\\pijiu_shop_eleme_new_address.csv";
        String tableName = "pijiu_shop_eleme_new_address_20250718";
        
        try {
            syncCsvToSQLServer(csvFilePath, tableName);
        } catch (Exception e) {
            System.err.println("同步失败: " + e.getMessage());
            e.printStackTrace();
        }
    }
    
    /**
     * 测试CSV解析逻辑
     */
    private static void testCSVParsing() {
        System.out.println("=== 测试CSV解析 ===");
        
        // 测试用例1：包含空字段的行
        String testLine1 = "福建省,福州市,小箬乡|闽侯县|福州市|福建省,好食期超市,545928469,月售9999+,9999,好食期,1191731,672910881195,16502705692221876,燕京啤酒特制 500毫升6罐装  特制,\"\",24.2,1,35,https://img.alicdn.com/imgextra/i2/6000000007983/O1CN012UqIgR28qGp1sgSXI_!!6000000007983-2-eleretail.png,2025-06-20 13:24:45";
        
        String[] result1 = parseCSVLine(testLine1);
        System.out.println("测试行1: " + testLine1);
        System.out.println("解析结果: " + Arrays.toString(result1));
        System.out.println("字段数量: " + result1.length);
        
        // 测试用例2：包含连续逗号的行
        String testLine2 = "a,b,,d,e";
        String[] result2 = parseCSVLine(testLine2);
        System.out.println("\n测试行2: " + testLine2);
        System.out.println("解析结果: " + Arrays.toString(result2));
        System.out.println("字段数量: " + result2.length);
        
        // 测试用例3：包含引号内逗号的行
        String testLine3 = "a,\"b,c\",d";
        String[] result3 = parseCSVLine(testLine3);
        System.out.println("\n测试行3: " + testLine3);
        System.out.println("解析结果: " + Arrays.toString(result3));
        System.out.println("字段数量: " + result3.length);
        
        // 测试用例4：包含特殊符号的行
        String testLine4 = "a,b\nwithNewline,c\twithTab,d\rwithReturn,e";
        String[] result4 = parseCSVLine(testLine4);
        System.out.println("\n测试行4: " + testLine4);
        System.out.println("解析结果: " + Arrays.toString(result4));
        System.out.println("字段数量: " + result4.length);
        
        // 测试用例5：模拟跨行字段合并
        String testLine5 = "安徽省,合肥市,琥珀街道|蜀山区|合肥市|安徽省,京东便利店(合肥包河店),20003262285,月售6000+,6000,京东便利店,569609,821985590,172284352,【冰镇可选】胖东来12度精酿小麦";
        String testLine5_part2 = "啤酒330ml／罐,8.8,6.8,100,1000,https://img.alicdn.com/imgextra/i3/1567219/O1CN01qEAdDm23CMAtk6FLE_!!1567219-0-eleretail.jpg,2025-06-19 17:15:03";
        String combinedLine = testLine5 + " " + testLine5_part2;
        String[] result5 = parseCSVLine(combinedLine);
        System.out.println("\n测试行5（模拟合并）: " + combinedLine);
        System.out.println("解析结果: " + Arrays.toString(result5));
        System.out.println("字段数量: " + result5.length);
        
        System.out.println("\n=== 测试完成 ===");
    }
    
    /**
     * 将CSV文件同步到SQL Server表
     * @param csvFilePath CSV文件路径
     * @param tableName 目标表名
     * @throws Exception 异常
     */
    public static void syncCsvToSQLServer(String csvFilePath, String tableName) throws Exception {
        System.out.println("开始同步CSV文件到SQL Server...");
        System.out.println("CSV文件路径: " + csvFilePath);
        System.out.println("目标表名: " + tableName);
        
        Connection connection = null;
        BufferedReader reader = null;
        
        try {
            // 连接SQL Server数据库
            connection = DriverManager.getConnection(SQLSERVER_URL, SQLSERVER_USER, SQLSERVER_PASSWORD);
            connection.setAutoCommit(false); // SQL Server使用事务
            System.out.println("成功连接到SQL Server数据库");
            
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
                connection.commit();
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
            
            connection.commit();
            System.out.println("CSV文件同步完成！");
            
        } catch (Exception e) {
            if (connection != null) {
                try {
                    connection.rollback();
                } catch (SQLException rollbackEx) {
                    System.err.println("回滚失败: " + rollbackEx.getMessage());
                }
            }
            throw e;
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
     * 解析CSV行，正确处理空字段、连续逗号、空引号等情况
     * @param line CSV行
     * @return 解析后的字段数组（保留所有字段包括空字段）
     */
    private static String[] parseCSVLine(String line) {
        List<String> result = new ArrayList<>();
        StringBuilder current = new StringBuilder();
        boolean inQuotes = false;
        
        for (int i = 0; i < line.length(); i++) {
            char c = line.charAt(i);
            
            if (c == '"') {
                // 检查是否是转义的引号
                if (i + 1 < line.length() && line.charAt(i + 1) == '"') {
                    // 连续两个引号，表示转义的引号，添加一个引号到结果中
                    current.append('"');
                    i++; // 跳过下一个引号
                } else {
                    // 单个引号，切换引号状态，不添加到结果中
                    inQuotes = !inQuotes;
                }
            } else if (c == ',' && !inQuotes) {
                // 遇到字段分隔符，添加当前字段（包括空字段）
                result.add(current.toString());
                current = new StringBuilder();
            } else {
                current.append(c);
            }
        }
        
        // 处理最后一个字段
        result.add(current.toString());
        
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
        String checkTableSQL = "SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = ?";
        try (PreparedStatement stmt = connection.prepareStatement(checkTableSQL)) {
            stmt.setString(1, tableName);
            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    return rs.getInt(1) > 0;
                }
            }
        }
        return false;
    }
    
    /**
     * 创建表，所有字段类型都设置为NVARCHAR(MAX)
     * @param connection 数据库连接
     * @param tableName 表名
     * @param columns 字段名数组
     * @throws SQLException SQL异常
     */
    private static void createTable(Connection connection, String tableName, String[] columns) throws SQLException {
        StringBuilder createTableSQL = new StringBuilder("CREATE TABLE [" + tableName + "] (");
        
        for (int i = 0; i < columns.length; i++) {
            String columnName = columns[i].replaceAll("[^a-zA-Z0-9_]", "_"); // 清理字段名
            createTableSQL.append("[").append(columnName).append("] NVARCHAR(MAX)");
            if (i < columns.length - 1) {
                createTableSQL.append(", ");
            }
        }
        
        createTableSQL.append(")");
        
        System.out.println("建表SQL: " + createTableSQL.toString());
        
        try (Statement statement = connection.createStatement()) {
            statement.execute(createTableSQL.toString());
        }
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
        String getColumnsSQL = "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = ? ORDER BY ORDINAL_POSITION";
        
        try (PreparedStatement stmt = connection.prepareStatement(getColumnsSQL)) {
            stmt.setString(1, tableName);
            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    columns.add(rs.getString("COLUMN_NAME"));
                }
            }
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
        StringBuilder insertSQL = new StringBuilder("INSERT INTO [" + tableName + "] (");
        
        for (int i = 0; i < columns.length; i++) {
            String columnName = columns[i].replaceAll("[^a-zA-Z0-9_]", "_");
            insertSQL.append("[").append(columnName).append("]");
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
        
        System.out.println("插入SQL: " + insertSQL.toString());
        
        try (PreparedStatement preparedStatement = connection.prepareStatement(insertSQL.toString())) {
            String line;
            int batchCount = 0;
            int totalCount = 0;
            
            System.out.println("开始插入数据...");
            
            while ((line = reader.readLine()) != null) {
                if (line.trim().isEmpty()) {
                    continue;
                }
                
                // 处理跨行字段 - 修改策略：基于字段数量而不是引号
                String completeLine = line;
                String[] values = parseCSVLine(completeLine);
                
                // 如果字段数量不足，尝试合并下一行
                while (values.length < columns.length) {
                    String nextLine = reader.readLine();
                    if (nextLine == null) {
                        System.err.println("文件结束时字段数量仍不足，跳过该行: " + completeLine);
                        break;
                    }
                    
                    // 智能合并：如果当前行不以逗号结尾，且下一行不以逗号开头，则用空格连接
                    // 否则直接连接
                    if (!completeLine.endsWith(",") && !nextLine.startsWith(",")) {
                        completeLine = completeLine + " " + nextLine;
                    } else {
                        completeLine = completeLine + nextLine;
                    }
                    
                    values = parseCSVLine(completeLine);
                    
                    // 防止无限循环
                    if (values.length > columns.length) {
                        System.err.println("合并后字段数量超出期望，可能存在数据问题: " + completeLine);
                        break;
                    }
                    
                    System.out.println("合并行后字段数量: " + values.length + " (目标: " + columns.length + ")");
                }
                
                // 检查字段数量是否匹配
                if (values.length != columns.length) {
                    System.err.println("字段数量不匹配，跳过该行 (期望: " + columns.length + ", 实际: " + values.length + ")");
                    System.err.println("行内容: " + completeLine);
                    System.err.println("解析结果: " + Arrays.toString(values));
                    continue;
                }
                
                for (int i = 0; i < values.length; i++) {
                    String value = values[i];
                    
                    // 清理字段值，去掉特殊符号
                    if (value != null) {
                        value = value.replaceAll("[\\r\\n\\t]", "").trim();
                    }
                    
                    // 正确处理空值和空字符串
                    if (value == null || value.isEmpty()) {
                        preparedStatement.setNull(i + 1, Types.NVARCHAR);
                    } else {
                        preparedStatement.setString(i + 1, value);
                    }
                }
                
                preparedStatement.addBatch();
                batchCount++;
                totalCount++;
                
                if (batchCount >= BATCH_SIZE) {
                    preparedStatement.executeBatch();
                    preparedStatement.clearBatch();
                    connection.commit(); // 定期提交
                    batchCount = 0;
                    System.out.println("已插入 " + totalCount + " 条记录");
                }
            }
            
            // 插入剩余的数据
            if (batchCount > 0) {
                preparedStatement.executeBatch();
                connection.commit();
            }
            
            System.out.println("数据插入完成，总共插入了 " + totalCount + " 条记录");
        }
    }
} 