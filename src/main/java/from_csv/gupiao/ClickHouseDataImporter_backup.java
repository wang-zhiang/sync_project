package from_csv.gupiao;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.sql.*;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

//同步到ck的
public class ClickHouseDataImporter_backup {
    
    // ClickHouse连接配置
    private static final String CLICKHOUSE_URL = "jdbc:clickhouse://hadoop110:8123/default";
    private static final String USERNAME = "default";
    private static final String PASSWORD = "smartpath";
    private static final String DATABASE = "tmp";
    private static final String TABLE_NAME = "jp_data";


    /*
    *
    * -- ods.us_data
      -- ods.zg_data

      -- ods.sp_data
      -- ods.jp_data
      -- ods.oy_data
      -- ods.adly_data
      -- ods.uk_data
    * */
    // CSV文件路径配置 - 在这里修改CSV文件路径
    private static final String CSV_FILE_PATH = "D:\\wzza\\develop\\idea_project\\ceshi\\src\\main\\java\\from_csv\\gupiao\\日本详情数据.csv";
    
    public static void main(String[] args) {
        try {
            ClickHouseDataImporter_backup importer = new ClickHouseDataImporter_backup();
            
            // 1. 创建表
            importer.createTable();
            
            // 2. 处理并导入数据
            importer.processAndImportData();
            
            System.out.println("数据导入完成！");
            
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
    /**
     * 创建ClickHouse表
     */
    /**
     * 创建ClickHouse表
     */
    public void createTable() throws SQLException {
        String createTableSQL = "CREATE TABLE IF NOT EXISTS %s.%s (" +
                "ymd String, " +
                "indicator String, " +
                "value Float64" +
                ") ENGINE = MergeTree() " +
                "ORDER BY ymd " +
                "SETTINGS index_granularity = 8192";
        
        try (Connection conn = DriverManager.getConnection(CLICKHOUSE_URL, USERNAME, PASSWORD);
             Statement stmt = conn.createStatement()) {
            
            // 删除已存在的表（确保这部分执行）
            String dropTableSQL = String.format("DROP TABLE IF EXISTS %s.%s", DATABASE, TABLE_NAME);
            stmt.execute(dropTableSQL);
            System.out.println("已删除旧表（如果存在）");
            
            // 创建新表
            stmt.execute(String.format(createTableSQL, DATABASE, TABLE_NAME));
            System.out.println("表创建成功: " + TABLE_NAME);
        }
    }
    
    /**
     * 处理CSV数据并导入ClickHouse
     */
    public void processAndImportData() throws IOException, SQLException {
        // 使用配置的CSV文件路径
        String inputFile = CSV_FILE_PATH;
        
        // 读取原始数据
        Map<LocalDate, Map<String, String>> dataMap = readOriginalData(inputFile);
        
        // 获取所有指标名称
        List<String> indicators = getIndicators(inputFile);
        
        // 批量插入数据
        insertDataToClickHouse(dataMap, indicators);
    }
    
    /**
     * 读取原始CSV数据
     */
    /**
     * 正确解析CSV行，处理带引号和逗号的字段
     */
    /**
     * 改进的CSV行解析方法，更好地处理引号和逗号
     */
    private String[] parseCSVLine(String line) {
        List<String> result = new ArrayList<>();
        boolean inQuotes = false;
        StringBuilder currentField = new StringBuilder();
        
        for (int i = 0; i < line.length(); i++) {
            char c = line.charAt(i);
            
            if (c == '"') {
                // 处理连续的双引号（转义）
                if (i + 1 < line.length() && line.charAt(i + 1) == '"' && inQuotes) {
                    currentField.append('"');
                    i++; // 跳过下一个引号
                } else {
                    inQuotes = !inQuotes;
                }
            } else if (c == ',' && !inQuotes) {
                result.add(currentField.toString().trim());
                currentField = new StringBuilder();
            } else {
                currentField.append(c);
            }
        }
        
        // 添加最后一个字段
        result.add(currentField.toString().trim());
        
        return result.toArray(new String[0]);
    }
    
    /**
     * 读取原始CSV数据 - 修复版本
     */
    private Map<LocalDate, Map<String, String>> readOriginalData(String filename) throws IOException {
        Map<LocalDate, Map<String, String>> dataMap = new HashMap<>();
        
        try (BufferedReader br = new BufferedReader(new FileReader(filename))) {
            String headerLine = br.readLine();
            String[] headers = parseCSVLine(headerLine);
            
            System.out.println("CSV列数: " + headers.length);
            for (int i = 0; i < headers.length; i++) {
                System.out.println("第" + (i+1) + "列: " + headers[i]);
            }
            
            String line;
            int lineCount = 0;
            while ((line = br.readLine()) != null) {
                lineCount++;
                if (lineCount % 5000 == 0) {
                    System.out.println("已读取 " + lineCount + " 行数据...");
                }
                
                String[] values = parseCSVLine(line);
                
                if (values.length > 0 && !values[0].trim().isEmpty()) {
                    try {
                        // 使用支持多种格式的日期解析
                        LocalDate date = parseDate(values[0]);
                        Map<String, String> dayData = new HashMap<>();
                        
                        // 调试：打印特定日期的解析结果
                        if ("2025/6/27".equals(values[0].trim())) {
                            System.out.println("调试 2025/6/27:");
                            System.out.println("解析后列数: " + values.length);
                            for (int i = 0; i < Math.min(values.length, headers.length); i++) {
                                System.out.println("第" + (i+1) + "列 [" + headers[i] + "]: [" + values[i] + "]");
                            }
                        }
                        
                        for (int i = 1; i < Math.min(values.length, headers.length); i++) {
                            String indicator = headers[i].trim();
                            String value = values[i].trim();
                            
                            if (value.isEmpty()) {
                                value = null;
                            } else {
                                // 清理数据：移除引号和多余空格
                                value = value.replace("\"", "").trim();
                                if (value.isEmpty()) {
                                    value = null;
                                }
                            }
                            dayData.put(indicator, value);
                        }
                        
                        dataMap.put(date, dayData);
                        
                        // 调试：验证数据是否正确存储
                        if ("2025/6/27".equals(values[0].trim())) {
                            System.out.println("存储的数据:");
                            for (Map.Entry<String, String> entry : dayData.entrySet()) {
                                if (entry.getValue() != null && !entry.getValue().isEmpty()) {
                                    System.out.println("  " + entry.getKey() + " = " + entry.getValue());
                                }
                            }
                        }
                        
                    } catch (Exception e) {
                        System.err.println("解析行失败: " + line.substring(0, Math.min(50, line.length())) + "..., 错误: " + e.getMessage());
                    }
                }
            }
        }
        
        System.out.println("CSV数据读取完成，共 " + dataMap.size() + " 天的数据");
        return dataMap;
    }
    
    /**
     * 解析多种日期格式
     */
    private LocalDate parseDate(String dateStr) {
        String cleanDate = dateStr.trim().replace("\"", "");
        
        DateTimeFormatter[] formatters = {
            DateTimeFormatter.ofPattern("yyyy/M/d"),    // 2025/6/27
            DateTimeFormatter.ofPattern("yyyy/MM/dd"),  // 2025/06/27
            DateTimeFormatter.ofPattern("yyyy-M-d"),    // 2025-6-27
            DateTimeFormatter.ofPattern("yyyy-MM-dd")   // 2025-06-27
        };
        
        for (DateTimeFormatter formatter : formatters) {
            try {
                return LocalDate.parse(cleanDate, formatter);
            } catch (Exception e) {
                // 继续尝试下一个格式
            }
        }
        
        throw new RuntimeException("无法解析日期格式: " + dateStr);
    }
    
    /**
     * 获取指标列表 - 修复版本
     */
    private List<String> getIndicators(String filename) throws IOException {
        List<String> indicators = new ArrayList<>();
        
        try (BufferedReader br = new BufferedReader(new FileReader(filename))) {
            String headerLine = br.readLine();
            String[] headers = parseCSVLine(headerLine); // 使用新的解析方法
            
            for (int i = 1; i < headers.length; i++) {
                indicators.add(headers[i].trim());
            }
        }
        
        System.out.println("共找到 " + indicators.size() + " 个指标");
        return indicators;
    }
    
    /**
     * 批量插入数据到ClickHouse
     */
    private void insertDataToClickHouse(Map<LocalDate, Map<String, String>> dataMap, 
                                       List<String> indicators) throws SQLException {
        
        LocalDate startDate = LocalDate.of(1928, 1, 1);
        LocalDate endDate = LocalDate.of(2025, 12, 31);
        
        String insertSQL = String.format("INSERT INTO %s.%s (ymd, indicator, value) VALUES (?, ?, ?)", 
                                        DATABASE, TABLE_NAME);  // 改为ymd
        
        try (Connection conn = DriverManager.getConnection(CLICKHOUSE_URL, USERNAME, PASSWORD)) {
            conn.setAutoCommit(false);
            
            try (PreparedStatement pstmt = conn.prepareStatement(insertSQL)) {
                
                int batchCount = 0;
                int totalRecords = 0;
                
                LocalDate currentDate = startDate;
                System.out.println("开始插入数据，日期范围：" + startDate + " 到 " + endDate);
                
                while (!currentDate.isAfter(endDate)) {
                    
                    // 每1000天打印一次进度
                    if (totalRecords % (indicators.size() * 1000) == 0) {
                        System.out.println("当前处理日期: " + currentDate + ", 已处理记录: " + totalRecords);
                    }
                    
                    Map<String, String> dayData = dataMap.get(currentDate);
                    

                    
                    // 在类的开头添加import
                    
                    // 修改insertDataToClickHouse方法中的数值处理部分
                    for (String indicator : indicators) {
                        double value = -99999999; // 默认值
                        
                        if (dayData != null && dayData.containsKey(indicator)) {
                            String originalValue = dayData.get(indicator);
                            if (originalValue != null && !originalValue.trim().isEmpty()) {
                                try {
                                    // 移除逗号和引号，然后解析数值
                                    String cleanValue = originalValue.trim()
                                        .replace("\"", "")  // 移除引号
                                        .replace(",", "");   // 移除逗号
                                    
                                    // 使用BigDecimal保持精度，然后转换为double
                                    BigDecimal bd = new BigDecimal(cleanValue);
                                    // 保留合理的小数位数，避免精度问题
                                    bd = bd.setScale(10, RoundingMode.HALF_UP);
                                    value = bd.doubleValue();
                                    
                                } catch (NumberFormatException e) {
                                    // 如果还是解析失败，打印调试信息
                                    System.out.println("无法解析数值: " + originalValue + " (指标: " + indicator + ", 日期: " + currentDate + ")");
                                    value = -1.0; // 解析失败使用默认值
                                }
                            }
                        }
                        
                        pstmt.setString(1, currentDate.toString());
                        pstmt.setString(2, indicator);
                        pstmt.setDouble(3, value);
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
                    }
                    
                    currentDate = currentDate.plusDays(1);
                }
                
                // 插入剩余的记录
                if (batchCount > 0) {
                    pstmt.executeBatch();
                    conn.commit();
                }
                
                System.out.println("数据插入完成！总共插入 " + totalRecords + " 条记录");
                System.out.println("日期范围：" + startDate + " 到 " + endDate);
            }
        }
    }
}