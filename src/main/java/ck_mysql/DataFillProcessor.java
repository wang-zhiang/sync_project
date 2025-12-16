package ck_mysql;

import java.sql.*;
import java.util.*;

public class DataFillProcessor {
    private static final String CLICKHOUSE_URL = "jdbc:clickhouse://hadoop110:8123";
    private static final String USERNAME = "default";
    private static final String PASSWORD = "smartpath";
    private static final String TABLE_NAME = "ods.us_data";
    private static final double INVALID_VALUE = -99999999.0;
    
    public static void main(String[] args) {
        try {
            fillMissingValues();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
    
    public static void fillMissingValues() throws SQLException {
        try (Connection conn = DriverManager.getConnection(CLICKHOUSE_URL, USERNAME, PASSWORD)) {
            
            // 获取所有不同的indicator
            Set<String> indicators = getDistinctIndicators(conn);
            System.out.println("找到 " + indicators.size() + " 个不同的indicator");
            
            // 为每个indicator处理数据补全
            for (String indicator : indicators) {
                processIndicator(conn, indicator);
            }
            
            System.out.println("✅ 数据补全完成！");
        }
    }
    
    private static Set<String> getDistinctIndicators(Connection conn) throws SQLException {
        Set<String> indicators = new HashSet<>();
        String sql = "SELECT DISTINCT indicator FROM " + TABLE_NAME;
        
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            while (rs.next()) {
                indicators.add(rs.getString("indicator"));
            }
        }
        return indicators;
    }
    
    private static void processIndicator(Connection conn, String indicator) throws SQLException {
        System.out.println("\n🔄 处理indicator: " + indicator);
        
        // 获取该indicator的所有数据，按日期排序
        String selectSql = "SELECT ymd, value FROM " + TABLE_NAME + 
                          " WHERE indicator = ? ORDER BY ymd";
        
        List<DataRecord> records = new ArrayList<>();
        try (PreparedStatement pstmt = conn.prepareStatement(selectSql)) {
            pstmt.setString(1, indicator);
            try (ResultSet rs = pstmt.executeQuery()) {
                while (rs.next()) {
                    records.add(new DataRecord(
                        rs.getString("ymd"),
                        rs.getDouble("value")
                    ));
                }
            }
        }
        
        if (records.isEmpty()) {
            System.out.println("  ⚠️ 该indicator无数据");
            return;
        }
        
        // 使用临时表方式更新（性能更好）
        int updateCount = forwardFillAndUpdateWithTempTable(conn, indicator, records);
        System.out.println("  ✅ 更新完成，共更新 " + updateCount + " 条记录");
    }
    
    private static int forwardFillAndUpdate(Connection conn, String indicator, 
                                          List<DataRecord> records) throws SQLException {
        String updateSql = "ALTER TABLE " + TABLE_NAME + 
                          " UPDATE value = ? WHERE indicator = ? AND ymd = ?";
        
        int updateCount = 0;
        Double lastValidValue = null;
        boolean foundFirstValid = false;
        
        try (PreparedStatement pstmt = conn.prepareStatement(updateSql)) {
            for (DataRecord record : records) {
                if (record.value != INVALID_VALUE) {
                    lastValidValue = record.value;
                    foundFirstValid = true;
                    System.out.println("  📅 " + record.ymd + ": 发现有效值 " + record.value);
                } else if (foundFirstValid && lastValidValue != null) {
                    pstmt.setDouble(1, lastValidValue);
                    pstmt.setString(2, indicator);
                    pstmt.setString(3, record.ymd);
                    pstmt.executeUpdate();
                    updateCount++;
                    System.out.println("  🔧 " + record.ymd + ": 补数为 " + lastValidValue);
                }
            }
        }
        
        return updateCount;
    }
    
    private static int forwardFillAndUpdateWithTempTable(Connection conn, String indicator, 
                                               List<DataRecord> records) throws SQLException {
        // 1. 收集需要更新的数据
        List<UpdateRecord> updateRecords = new ArrayList<>();
        Double lastValidValue = null;
        boolean foundFirstValid = false;
        
        for (DataRecord record : records) {
            if (record.value != INVALID_VALUE) {
                lastValidValue = record.value;
                foundFirstValid = true;
                System.out.println("  📅 " + record.ymd + ": 发现有效值 " + record.value);
            } else if (foundFirstValid && lastValidValue != null) {
                updateRecords.add(new UpdateRecord(record.ymd, lastValidValue));
                System.out.println("  🔧 " + record.ymd + ": 待补数为 " + lastValidValue);
            }
        }
        
        if (updateRecords.isEmpty()) {
            System.out.println("  ✅ 无需补数");
            return 0;
        }
        
        // 2. 按值分组，实现真正的批量更新
        Map<Double, List<String>> valueToYmds = new HashMap<>();
        for (UpdateRecord record : updateRecords) {
            valueToYmds.computeIfAbsent(record.value, k -> new ArrayList<>()).add(record.ymd);
        }
        
        System.out.println("  🚀 开始真正的批量更新，共 " + valueToYmds.size() + " 个不同的值...");
        long startTime = System.currentTimeMillis();
        int totalUpdated = 0;
        
        try (Statement stmt = conn.createStatement()) {
            for (Map.Entry<Double, List<String>> entry : valueToYmds.entrySet()) {
                Double value = entry.getKey();
                List<String> ymds = entry.getValue();
                
                // 构建IN子句
                StringBuilder ymdList = new StringBuilder();
                for (int i = 0; i < ymds.size(); i++) {
                    if (i > 0) ymdList.append(", ");
                    ymdList.append("'").append(ymds.get(i)).append("'");
                }
                
                // 一条SQL更新相同值的所有日期
                String updateSql = String.format(
                    "ALTER TABLE %s UPDATE value = %f WHERE indicator = '%s' AND ymd IN (%s)",
                    TABLE_NAME, value, indicator, ymdList.toString()
                );
                
                System.out.println("  📝 批量更新值 " + value + " 的 " + ymds.size() + " 个日期");
                stmt.execute(updateSql);
                totalUpdated += ymds.size();
            }
        }
        
        long endTime = System.currentTimeMillis();
        System.out.println("  ✅ 真正批量更新完成！共 " + totalUpdated + 
                         " 条记录，" + valueToYmds.size() + " 条SQL，耗时 " + 
                         (endTime - startTime) + "ms");
        
        return totalUpdated;
    }
    
    // 数据记录类
    private static class DataRecord {
        String ymd;
        double value;
        
        DataRecord(String ymd, double value) {
            this.ymd = ymd;
            this.value = value;
        }
    }
    
    // 更新记录类（之前缺少这个类定义）
    private static class UpdateRecord {
        String ymd;
        double value;
        
        UpdateRecord(String ymd, double value) {
            this.ymd = ymd;
            this.value = value;
        }
    }
}