package from_csv.gupiao;

import java.io.*;
import java.sql.*;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.*;

public class ForexDataImporter {

 //补日期版  外汇历史行情
    private static final String CLICKHOUSE_URL = "jdbc:clickhouse://hadoop110:8123/default";
    private static final String USERNAME = "default";
    private static final String PASSWORD = "smartpath";
    private static final String DATABASE = "ods";
    private static final String TABLE_NAME = "forex_data5";
    
    public static void main(String[] args) {
        try {
            ForexDataImporter importer = new ForexDataImporter();
            
            // 1. 创建表
            importer.createTable();
            
            // 2. 处理并导入数据
            importer.processAndImportData();
            
            System.out.println("外汇数据导入完成！");
            
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
    /**
     * 创建ClickHouse表
     */
    public void createTable() throws SQLException {
        String createTableSQL = String.format(
            "CREATE TABLE IF NOT EXISTS %s.%s (" +
            "ymd String, " +
            "code String, " +
            "open Float64, " +
            "high Float64, " +
            "low Float64, " +
            "close Float64, " +
            "pct_chg Float64" +
            ") ENGINE = MergeTree() " +
            "ORDER BY (ymd, code) " +
            "SETTINGS index_granularity = 8192",
            DATABASE, TABLE_NAME
        );
        
        try (Connection conn = DriverManager.getConnection(CLICKHOUSE_URL, USERNAME, PASSWORD);
             Statement stmt = conn.createStatement()) {
            
            // 删除已存在的表
            String dropTableSQL = String.format("DROP TABLE IF EXISTS %s.%s", DATABASE, TABLE_NAME);
            stmt.execute(dropTableSQL);
            System.out.println("已删除旧表（如果存在）");
            
            // 创建新表
            stmt.execute(createTableSQL);
            System.out.println("表创建成功: " + TABLE_NAME);
        }
    }
    
    /**
     * 处理并导入数据
     */
    public void processAndImportData() throws IOException, SQLException {
        String filename = "D:\\wzza\\develop\\idea_project\\ceshi\\src\\main\\java\\from_csv\\gupiao\\外汇历史行情5.csv";
        
        // 从CSV中提取code
        String extractedCode = extractCodeFromCSV(filename);
        System.out.println("从CSV中提取到的code: " + extractedCode);
        
        // 读取原始数据
        Map<LocalDate, ForexRecord> dataMap = readOriginalData(filename);
        
        // 插入数据到ClickHouse（包含日期补全）
        insertDataToClickHouse(dataMap, extractedCode);
    }
    
    /**
     * 从CSV文件中提取code值
     */
    private String extractCodeFromCSV(String filename) throws IOException {
        try (BufferedReader br = new BufferedReader(new FileReader(filename))) {
            String headerLine = br.readLine(); // 跳过表头
            String firstDataLine = br.readLine();
            
            if (firstDataLine != null) {
                String[] values = firstDataLine.split(",", -1);
                if (values.length >= 2) {
                    return values[1].trim(); // code在第二列
                }
            }
        }
        throw new RuntimeException("无法从CSV文件中提取code值");
    }
    
    /**
     * 外汇记录数据结构
     */
    private static class ForexRecord {
        double open = -99999999.0;
        double high = -99999999.0;
        double low = -99999999.0;
        double close = -99999999.0;
        double pctChg = -99999999.0;
        
        ForexRecord(String openStr, String highStr, String lowStr, String closeStr, String pctChgStr) {
            this.open = parseDouble(openStr);
            this.high = parseDouble(highStr);
            this.low = parseDouble(lowStr);
            this.close = parseDouble(closeStr);
            this.pctChg = parseDouble(pctChgStr);
        }
        
        private double parseDouble(String value) {
            if (value == null || value.trim().isEmpty()) {
                return -99999999.0;
            }
            try {
                // 处理可能的逗号分隔符
                String cleanValue = value.trim().replace(",", "");
                return Double.parseDouble(cleanValue);
            } catch (NumberFormatException e) {
                return -99999999.0;
            }
        }
    }
    
    /**
     * 读取原始CSV数据
     */
    private Map<LocalDate, ForexRecord> readOriginalData(String filename) throws IOException {
        Map<LocalDate, ForexRecord> dataMap = new HashMap<>();
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
        
        try (BufferedReader br = new BufferedReader(new FileReader(filename))) {
            String headerLine = br.readLine(); // 跳过表头
            String line;
            
            while ((line = br.readLine()) != null) {
                String[] values = line.split(",", -1); // -1保留空字符串
                
                if (values.length >= 6) {
                    try {
                        LocalDate date = LocalDate.parse(values[0], formatter);
                        ForexRecord record = new ForexRecord(
                            values[2], // open
                            values[3], // high
                            values[4], // low
                            values[5], // close
                            values.length > 6 ? values[6] : "" // pct_chg
                        );
                        dataMap.put(date, record);
                    } catch (Exception e) {
                        System.out.println("跳过无效行: " + line);
                    }
                }
            }
        }
        
        System.out.println("读取到 " + dataMap.size() + " 天的原始数据");
        return dataMap;
    }
    
    /**
     * 插入数据到ClickHouse，包含日期补全
     */
    private void insertDataToClickHouse(Map<LocalDate, ForexRecord> dataMap, String code) throws SQLException {
        LocalDate startDate = LocalDate.of(1990, 12, 19);
        LocalDate endDate = LocalDate.of(2025, 9, 16);
        
        String insertSQL = String.format(
            "INSERT INTO %s.%s (ymd, code, open, high, low, close, pct_chg) VALUES (?, ?, ?, ?, ?, ?, ?)",
            DATABASE, TABLE_NAME
        );
        
        try (Connection conn = DriverManager.getConnection(CLICKHOUSE_URL, USERNAME, PASSWORD)) {
            conn.setAutoCommit(false);
            
            try (PreparedStatement pstmt = conn.prepareStatement(insertSQL)) {
                
                int batchCount = 0;
                int totalRecords = 0;
                
                LocalDate currentDate = startDate;
                System.out.println("开始插入数据，日期范围：" + startDate + " 到 " + endDate);
                System.out.println("使用code: " + code);
                
                while (!currentDate.isAfter(endDate)) {
                    
                    // 每1000天打印一次进度
                    if (totalRecords % 1000 == 0) {
                        System.out.println("当前处理日期: " + currentDate + ", 已处理记录: " + totalRecords);
                    }
                    
                    // 获取当天数据，如果没有则使用默认值(-1)
                    ForexRecord record = dataMap.getOrDefault(currentDate, new ForexRecord("", "", "", "", ""));
                    
                    pstmt.setString(1, currentDate.toString());
                    pstmt.setString(2, code); // 使用从CSV提取的code
                    pstmt.setDouble(3, record.open);
                    pstmt.setDouble(4, record.high);
                    pstmt.setDouble(5, record.low);
                    pstmt.setDouble(6, record.close);
                    pstmt.setDouble(7, record.pctChg);
                    pstmt.addBatch();
                    
                    batchCount++;
                    totalRecords++;
                    
                    // 每10000条记录执行一次批量插入
                    if (batchCount >= 10000) {
                        pstmt.executeBatch();
                        conn.commit();
                        System.out.println("已插入 " + totalRecords + " 条记录，当前日期: " + currentDate);
                        batchCount = 0;
                    }
                    
                    currentDate = currentDate.plusDays(1);
                }
                
                // 插入剩余的记录
                if (batchCount > 0) {
                    pstmt.executeBatch();
                    conn.commit();
                }
                
                System.out.println("数据插入完成，总记录数: " + totalRecords);
            }
        }
    }
}