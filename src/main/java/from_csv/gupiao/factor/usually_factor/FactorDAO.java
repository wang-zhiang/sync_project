package from_csv.gupiao.factor.usually_factor;

import java.sql.*;
import java.time.LocalDate;
import java.util.*;

public class FactorDAO {
    
    /**
     * 获取factor表中的所有记录
     */
    public static List<FactorRecord> getFactorRecords() throws SQLException {
        List<FactorRecord> records = new ArrayList<>();
        String sql = "SELECT TitleEn, formula FROM ods.factor WHERE (formula not like  '%期%' and  formula not like  '%月%' ) ";
        //String sql = "SELECT TitleEn, formula FROM ods.factor WHERE NOT match(formula, '[\\\\u4e00-\\\\u9fff]')";
        //String sql = "SELECT TitleEn, formula FROM ods.factor WHERE  TitleEn = 'CurrentAccountToGDP_Diff_US_JP'";

        try (Connection conn = DatabaseConfig.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql);
             ResultSet rs = stmt.executeQuery()) {
            
            while (rs.next()) {
                records.add(new FactorRecord(
                    rs.getString("TitleEn"),
                    rs.getString("formula")
                ));
            }
        }
        
        return records;
    }
    
    /**
     * 获取指定日期和指标的数值
     */
    public static Map<String, Double> getIndicatorValues(LocalDate date, Set<String> indicators) throws SQLException {
        if (indicators.isEmpty()) {
            return new HashMap<>();
        }
        
        Map<String, Double> values = new HashMap<>();
        StringBuilder sql = new StringBuilder(
            "SELECT indicator, value FROM tmp.combined_all_data WHERE ymd = ? AND indicator IN (");
        
        for (int i = 0; i < indicators.size(); i++) {
            if (i > 0) sql.append(", ");
            sql.append("?");
        }
        sql.append(")");
        
        try (Connection conn = DatabaseConfig.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql.toString())) {
            
            stmt.setString(1, date.toString());
            int paramIndex = 2;
            for (String indicator : indicators) {
                stmt.setString(paramIndex++, indicator);
            }
            
            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    values.put(rs.getString("indicator"), rs.getDouble("value"));
                }
            }
        }
        
        return values;
    }
    
    /**
     * 批量获取日期范围内的指标数值
     */
    public static Map<LocalDate, Map<String, Double>> getIndicatorValuesForDateRange(
            LocalDate startDate, LocalDate endDate, Set<String> indicators) throws SQLException {
        
        Map<LocalDate, Map<String, Double>> result = new HashMap<>();
        
        StringBuilder sql = new StringBuilder(
            "SELECT ymd, indicator, value FROM tmp.combined_all_data WHERE ymd BETWEEN ? AND ? AND indicator IN (");
        
        for (int i = 0; i < indicators.size(); i++) {
            if (i > 0) sql.append(", ");
            sql.append("?");
        }
        sql.append(") ORDER BY ymd");
        
        try (Connection conn = DatabaseConfig.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql.toString())) {
            
            stmt.setString(1, startDate.toString());
            stmt.setString(2, endDate.toString());
            int paramIndex = 3;
            for (String indicator : indicators) {
                stmt.setString(paramIndex++, indicator);
            }
            
            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    LocalDate date = LocalDate.parse(rs.getString("ymd"));
                    String indicator = rs.getString("indicator");
                    double value = rs.getDouble("value");
                    
                    result.computeIfAbsent(date, k -> new HashMap<>()).put(indicator, value);
                }
            }
        }
        
        return result;
    }
    
    /**
     * 一次性获取所有日期的指标数据（最重要的优化）
     */
    public static Map<LocalDate, Map<String, Double>> getAllIndicatorValues(Set<String> indicators) throws SQLException {
        Map<LocalDate, Map<String, Double>> result = new HashMap<>();
        
        StringBuilder sql = new StringBuilder(
            "SELECT ymd, indicator, value FROM tmp.combined_all_data WHERE ymd BETWEEN '1928-01-01' AND '2025-12-31' AND indicator IN (");
        
        for (int i = 0; i < indicators.size(); i++) {
            if (i > 0) sql.append(", ");
            sql.append("?");
        }
        sql.append(") ORDER BY ymd");
        
        try (Connection conn = DatabaseConfig.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql.toString())) {
            
            int paramIndex = 1;
            for (String indicator : indicators) {
                stmt.setString(paramIndex++, indicator);
            }
            
            System.out.println("🔍 执行一次性数据查询...");
            long queryStart = System.nanoTime();
            
            try (ResultSet rs = stmt.executeQuery()) {
                int rowCount = 0;
                while (rs.next()) {
                    LocalDate date = LocalDate.parse(rs.getString("ymd"));
                    String indicator = rs.getString("indicator");
                    double value = rs.getDouble("value");
                    
                    result.computeIfAbsent(date, k -> new HashMap<>()).put(indicator, value);
                    rowCount++;
                }
                
                long queryTime = (System.nanoTime() - queryStart) / 1_000_000;
                System.out.println(String.format("✅ 查询完成：%d行数据，耗时：%dms", rowCount, queryTime));
            }
        }
        
        return result;
    }
    
    /**
     * 创建结果表（如果不存在）
     */
    public static void createResultTableIfNotExists(String tableName) throws SQLException {
        String sql = String.format(
            "CREATE TABLE IF NOT EXISTS ods.%s (" +
            "ymd String, " +
            "TitleEn String, " +
            "calculated_value Float64" +
            ") ENGINE = MergeTree() " +
            "ORDER BY (ymd, TitleEn)",
            tableName
        );
        
        try (Connection conn = DatabaseConfig.getConnection();
             Statement stmt = conn.createStatement()) {
            stmt.execute(sql);
        }
    }
    
    /**
     * 批量插入计算结果
     */
    public static void insertResults(String tableName, List<CalculationResult> results) throws SQLException {
        if (results.isEmpty()) return;
        
        // 修改：支持完整表名（如 tmp.complex_factor_results_20251012）
        String sql;
        if (tableName.contains(".")) {
            // 如果表名包含数据库前缀，直接使用
            sql = String.format(
                "INSERT INTO %s (ymd, TitleEn, calculated_value) VALUES (?, ?, ?)",
                tableName
            );
        } else {
            // 如果没有数据库前缀，添加 ods 前缀（保持向后兼容）
            sql = String.format(
                "INSERT INTO ods.%s (ymd, TitleEn, calculated_value) VALUES (?, ?, ?)",
                tableName
            );
        }
        
        try (Connection conn = DatabaseConfig.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            
            for (CalculationResult result : results) {
                stmt.setString(1, result.getDate().toString());
                stmt.setString(2, result.getTitleEn());
                stmt.setDouble(3, result.getValue());
                stmt.addBatch();
            }
            
            stmt.executeBatch();
            System.out.println("✅ 成功插入 " + results.size() + " 条记录到 " + tableName);
        }
    }
    
    /**
     * Factor记录实体类
     */
    public static class FactorRecord {
        private final String titleEn;
        private final String formula;
        
        public FactorRecord(String titleEn, String formula) {
            this.titleEn = titleEn;
            this.formula = formula;
        }
        
        public String getTitleEn() { return titleEn; }
        public String getFormula() { return formula; }
    }
    
    /**
     * 计算结果实体类
     */
    public static class CalculationResult {
        private final LocalDate date;
        private final String titleEn;
        private final double value;
        
        public CalculationResult(LocalDate date, String titleEn, double value) {
            this.date = date;
            this.titleEn = titleEn;
            this.value = Math.round(value * 1000000.0) / 1000000.0; // 保留6位小数
        }
        
        public LocalDate getDate() { return date; }
        public String getTitleEn() { return titleEn; }
        public double getValue() { return value; }
    }
}