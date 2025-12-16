package from_csv.csvtosqlserver;
import java.io.*;
import java.sql.*;
import java.util.*;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

public class CsvToSqlServer {
    
    // 配置变量 - 请根据需要修改
    private static final String TABLE_NAME = "jd_normal_20251211"; // 目标表名
    private static final String CSV_FILE_PATH = "D:\\wzza\\develop\\idea_project\\ceshi\\src\\main\\java\\from_csv\\csvtosqlserver\\data\\jd_normal.csv"; // CSV文件路径
    private static final String DB_URL = "jdbc:sqlserver://192.168.3.183;databaseName=SourceDate;";
    private static final String DB_USER = "sa";
    private static final String DB_PASSWORD = "smartpthdata";
    
    // 表存在时的处理方式

    // OVERWRITE 好像不可用，error是可用的


    public enum TableExistsAction {
        ERROR,    // 报错
        OVERWRITE, // 覆盖（删除重建）
        APPEND     // 追加数据
    }
    
    private static final TableExistsAction TABLE_EXISTS_ACTION = TableExistsAction.ERROR;
    
    public static void main(String[] args) {
        CsvToSqlServer csvSync = new CsvToSqlServer();
        try {
            csvSync.syncCsvToSqlServer();
            System.out.println("CSV数据同步完成！");
        } catch (Exception e) {
            System.err.println("同步过程中发生错误: " + e.getMessage());
            e.printStackTrace();
        }
    }
    
    public void syncCsvToSqlServer() throws Exception {
        // 1. 读取CSV文件头部，获取字段信息
        List<String> headers = readCsvHeaders(CSV_FILE_PATH);
        System.out.println("检测到CSV字段: " + headers);
        
        // 2. 连接数据库
        try (Connection conn = DriverManager.getConnection(DB_URL, DB_USER, DB_PASSWORD)) {
            System.out.println("数据库连接成功！");
            
            // 3. 检查表是否存在
            boolean tableExists = checkTableExists(conn, TABLE_NAME);
            
            if (tableExists) {
                handleExistingTable(conn, TABLE_NAME);
            } else {
                // 4. 创建表
                createTable(conn, TABLE_NAME, headers);
            }
            
            // 5. 插入数据
            if (TABLE_EXISTS_ACTION != TableExistsAction.OVERWRITE || !tableExists) {
                insertCsvData(conn, TABLE_NAME, CSV_FILE_PATH, headers);
            }
        }
    }
    
    // 如需分号 CSV，请改为 ';'
    private static final char CSV_DELIMITER = ',';

    private List<String> readCsvHeaders(String csvFilePath) throws IOException {
        try (Reader reader = new BufferedReader(
                 new InputStreamReader(new FileInputStream(csvFilePath), java.nio.charset.StandardCharsets.UTF_8));
             CSVParser parser = CSVFormat.DEFAULT
                 .withDelimiter(CSV_DELIMITER)
                 .withQuote('"')
                 .withIgnoreSurroundingSpaces(true)
                 .withTrim(true)
                 .withFirstRecordAsHeader()
                 .parse(reader)) {

            List<String> headers = new ArrayList<>();
            for (String h : parser.getHeaderMap().keySet()) {
                headers.add(h.replace("\uFEFF", "")); // 去除可能的 BOM
            }
            return headers;
        }
    }
    
    private boolean checkTableExists(Connection conn, String tableName) throws SQLException {
        DatabaseMetaData metaData = conn.getMetaData();
        try (ResultSet rs = metaData.getTables(null, null, tableName.toUpperCase(), new String[]{"TABLE"})) {
            return rs.next();
        }
    }
    
    private void handleExistingTable(Connection conn, String tableName) throws SQLException {
        switch (TABLE_EXISTS_ACTION) {
            case ERROR:
                throw new SQLException("表 '" + tableName + "' 已存在！请检查表名或修改处理方式。");
            case OVERWRITE:
                System.out.println("表已存在，正在删除重建...");
                try (Statement stmt = conn.createStatement()) {
                    stmt.executeUpdate("DROP TABLE " + tableName);
                }
                break;
            case APPEND:
                System.out.println("表已存在，将追加数据...");
                break;
        }
    }
    
    private void createTable(Connection conn, String tableName, List<String> headers) 
            throws SQLException, IOException {

        StringBuilder createTableSQL = new StringBuilder();
        createTableSQL.append("CREATE TABLE ").append(tableName).append(" (\n");

        for (int i = 0; i < headers.size(); i++) {
            String columnName = sanitizeColumnName(headers.get(i));
            createTableSQL.append("    [")
                         .append(columnName)
                         .append("] NVARCHAR(MAX)");
            if (i < headers.size() - 1) {
                createTableSQL.append(",");
            }
            createTableSQL.append("\n");
        }

        createTableSQL.append(")");

        System.out.println("创建表SQL: " + createTableSQL.toString());

        try (Statement stmt = conn.createStatement()) {
            stmt.executeUpdate(createTableSQL.toString());
            System.out.println("表 '" + tableName + "' 创建成功！");
        }
    }
    
    private String sanitizeColumnName(String columnName) {
        // 处理中文和特殊字符的列名
        return columnName.replaceAll("[^\\w\\u4e00-\\u9fa5]", "_");
    }
    
    private void insertCsvData(Connection conn, String tableName, String csvFilePath, List<String> headers) 
            throws SQLException, IOException {

        // 构建插入SQL（按 header 映射对应的 SQL 列名）
        StringBuilder insertSQL = new StringBuilder();
        insertSQL.append("INSERT INTO ").append(tableName).append(" (");
        for (int i = 0; i < headers.size(); i++) {
            insertSQL.append("[").append(sanitizeColumnName(headers.get(i))).append("]");
            if (i < headers.size() - 1) {
                insertSQL.append(", ");
            }
        }
        insertSQL.append(") VALUES (");
        for (int i = 0; i < headers.size(); i++) {
            insertSQL.append("?");
            if (i < headers.size() - 1) {
                insertSQL.append(", ");
            }
        }
        insertSQL.append(")");

        try (PreparedStatement pstmt = conn.prepareStatement(insertSQL.toString());
             Reader reader = new BufferedReader(
                 new InputStreamReader(new FileInputStream(csvFilePath), java.nio.charset.StandardCharsets.UTF_8));
             CSVParser parser = CSVFormat.DEFAULT
                 .withDelimiter(CSV_DELIMITER)
                 .withQuote('"')
                 .withIgnoreSurroundingSpaces(true)
                 .withTrim(true)
                 .withFirstRecordAsHeader()
                 .parse(reader)) {

            conn.setAutoCommit(false);
            int batchCount = 0;
            int totalCount = 0;

            for (CSVRecord record : parser) {
                for (int i = 0; i < headers.size(); i++) {
                    String val = record.get(headers.get(i)); // 按表头名取值，避免错位
                    if (val == null || val.isEmpty()) {
                        pstmt.setNull(i + 1, Types.NVARCHAR);
                    } else {
                        pstmt.setString(i + 1, val);
                    }
                }
                pstmt.addBatch();
                batchCount++;
                totalCount++;

                // 可根据需要调整批次大小
                if (batchCount >= 1000) {
                    pstmt.executeBatch();
                    conn.commit();
                    batchCount = 0;
                    System.out.println("已插入 " + totalCount + " 条记录...");
                }
            }

            if (batchCount > 0) {
                pstmt.executeBatch();
                conn.commit();
            }

            System.out.println("数据插入完成，共插入 " + totalCount + " 条记录。");
        }
    }
}