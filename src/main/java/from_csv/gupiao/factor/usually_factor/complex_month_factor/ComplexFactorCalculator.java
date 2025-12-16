package from_csv.gupiao.factor.usually_factor.complex_month_factor;

import from_csv.gupiao.factor.usually_factor.DatabaseConfig;
import from_csv.gupiao.factor.usually_factor.FactorDAO;

import java.sql.*;
import java.time.LocalDate;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 复杂因子计算器 - 处理包含"本月"和"上年同月"的因子
 */
public class ComplexFactorCalculator {
    
    // 在main函数中定义的表名
    private static String SOURCE_TABLE_NAME = "tmp.combined_all_data_for_bq";
    private static final String RESULT_TABLE_NAME = "tmp.complex_factor_month_20251012";
    
    private static final LocalDate START_DATE = LocalDate.of(1928, 1, 1);
    private static final LocalDate END_DATE = LocalDate.of(2025, 12, 31);
    private static final int BATCH_SIZE = 5000;
    private static final int PROGRESS_INTERVAL = 365;
    
    /**
     * 获取所有指标数据（使用自定义表名）
     */
    private static Map<LocalDate, Map<String, Double>> getAllIndicatorValuesFromCustomTable(
            Set<String> indicators, String tableName) throws SQLException {
        Map<LocalDate, Map<String, Double>> result = new HashMap<>();
        
        StringBuilder sql = new StringBuilder(
            "SELECT ymd, indicator, value FROM " + tableName + 
            " WHERE ymd BETWEEN '1928-01-01' AND '2025-12-31' AND indicator IN (");
        
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
            
            System.out.println("🔍 从表 [" + tableName + "] 执行一次性数据查询...");
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
     * 获取包含"本期"和"上期"的复杂因子
     */
    public static List<FactorDAO.FactorRecord> getComplexFactorRecords() throws SQLException {
        List<FactorDAO.FactorRecord> records = new ArrayList<>();
        // 🔧 修改：查询包含"本月"的因子，而不是"本期"
        String sql = "SELECT TitleEn, formula FROM ods.factor WHERE formula LIKE '%本月%'";
        
        try (Connection conn = DatabaseConfig.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql);
             ResultSet rs = stmt.executeQuery()) {
            
            while (rs.next()) {
                records.add(new FactorDAO.FactorRecord(
                    rs.getString("TitleEn"),
                    rs.getString("formula")
                ));
            }
        }
        
        return records;
    }
    
    /**
     * 处理单个复杂因子（优化版本）
     */
    private static void processComplexFactor(FactorDAO.FactorRecord factor) throws SQLException {
        System.out.println("\n=== 处理复杂因子: " + factor.getTitleEn() + " ===");
        
        // 提取基础指标
        String baseIndicator = ComplexFormulaParser.extractBaseIndicator(factor.getFormula());
        if (baseIndicator == null) {
            System.err.println("❌ 无法提取基础指标");
            return;
        }
        
        Set<String> indicators = new HashSet<>();
        indicators.add(baseIndicator);
        
        // 🚀 使用自定义表名获取数据
        System.out.println("📊 开始获取所有历史数据...");
        Map<LocalDate, Map<String, Double>> allData = getAllIndicatorValuesFromCustomTable(indicators, SOURCE_TABLE_NAME);
        System.out.println("✅ 数据获取完成，开始计算...");
        
        List<FactorDAO.CalculationResult> results = new ArrayList<>();
        LocalDate currentDate = START_DATE;
        int processedCount = 0;
    
        while (!currentDate.isAfter(END_DATE)) {
            String calculableFormula = null;
    
            try {
                // 从内存中获取本月值
                Map<String, Double> currentValues = allData.get(currentDate);
                if (currentValues == null) {
                    currentValues = new HashMap<>();
                }
                Double currentValue = currentValues.get(baseIndicator);
    
                if (currentValue != null && currentValue != -99999999.0) {
                    // 🔧 修改：调用新的上年同月查找方法
                    Double previousYearValue = findPreviousYearSameMonthValueFromMemory(baseIndicator, currentDate, allData);
    
                    if (previousYearValue != null && previousYearValue != -99999999.0) {
                        // 计算结果
                        calculableFormula = ComplexFormulaParser.replacePeriodsWithValues(
                            factor.getFormula(), baseIndicator, currentValue, previousYearValue);
                        double result = ComplexFormulaParser.evaluateExpression(calculableFormula);
                        
                        if (result != -99999999.0) {
                            results.add(new FactorDAO.CalculationResult(currentDate, factor.getTitleEn(), result));
                        } else {
                            results.add(new FactorDAO.CalculationResult(currentDate, factor.getTitleEn(), -99999999.0));
                        }
                    } else {
                        // 🔧 修改：上年同月数据无效时，直接设置为-99999999
                        results.add(new FactorDAO.CalculationResult(currentDate, factor.getTitleEn(), -99999999.0));
                        calculableFormula = "上年同月数据无效";
                    }
                } else {
                    // 🔧 修改：本月数据无效时，直接设置为-99999999
                    results.add(new FactorDAO.CalculationResult(currentDate, factor.getTitleEn(), -99999999.0));
                    calculableFormula = "本月数据无效";
                }
                
                // 批量插入
                if (results.size() >= BATCH_SIZE) {
                    FactorDAO.insertResults(RESULT_TABLE_NAME, results);
                    results.clear();
                }
                
            } catch (Exception e) {
                // 🔧 修复：遇到解析错误时停止程序，不再跳过
                System.err.println("❌ 复杂因子解析错误，程序终止！");
                System.err.println("错误因子: " + factor.getTitleEn());
                System.err.println("错误日期: " + currentDate);
                System.err.println("错误信息: " + e.getMessage());
                System.err.println("基础指标: " + baseIndicator);
                if (calculableFormula != null) {
                    System.err.println("解析公式: " + calculableFormula);
                }
                e.printStackTrace();
                System.exit(1); // 终止程序
            }
            
            processedCount++;
            if (processedCount % PROGRESS_INTERVAL == 0) {
                // 🆕 增强进度显示：包含解析后的公式
                String progressInfo = String.format("进度: %d天, 当前: %s", processedCount, currentDate);
                
                // 如果有解析后的公式，显示出来
                if (calculableFormula != null) {
                    progressInfo += String.format(" | 解析公式: [%s]", calculableFormula);
                } else {
                    progressInfo += " | 解析公式: [无数据]";
                }
                
                System.out.println(progressInfo);
            }
            
            currentDate = currentDate.plusDays(1);
        }
        
        // 处理剩余结果
        if (!results.isEmpty()) {
            FactorDAO.insertResults(RESULT_TABLE_NAME, results);
        }
    }
    
    /**
     * 从内存数据中查找上期有效值
     */
    /**
     * 🔧 新方法：从内存数据中查找上年同月的有效值
     */
    private static Double findPreviousYearSameMonthValueFromMemory(String indicator, LocalDate currentDate, 
                                                                  Map<LocalDate, Map<String, Double>> allData) {
        // 计算上一年的同月同日
        LocalDate previousYearDate = currentDate.minusYears(1);
        
        // 首先尝试精确匹配上年同月同日
        Map<String, Double> dayData = allData.get(previousYearDate);
        if (dayData != null) {
            Double value = dayData.get(indicator);
            if (value != null && value != -99999999.0) {
                return value;
            }
        }
        
        // 🔧 新逻辑：如果精确日期没有数据，在上年同月的所有日期中查找有效数据
        int previousYear = previousYearDate.getYear();
        int previousMonth = previousYearDate.getMonthValue();
        
        // 遍历上年同月的所有日期（1-31日）
        for (int day = 1; day <= 31; day++) {
            try {
                LocalDate searchDate = LocalDate.of(previousYear, previousMonth, day);
                dayData = allData.get(searchDate);
                if (dayData != null) {
                    Double value = dayData.get(indicator);
                    if (value != null && value != -99999999.0) {
                        return value;
                    }
                }
            } catch (Exception e) {
                // 忽略无效日期（如2月30日），继续查找
                continue;
            }
        }
        
        // 如果整个上年同月都没有有效数据，返回null
        return null;
    }
    

    /**
     * 创建复杂因子结果表（如果不存在）
     */
    private static void createResultTableIfNotExists(String tableName) throws SQLException {
        // 修正ClickHouse语法 - 分离数据库名和表名
        String[] parts = tableName.split("\\.");
        String dbName = parts[0];
        String tblName = parts[1];
        
        String sql = "CREATE TABLE IF NOT EXISTS " + dbName + "." + tblName + " (" +
                    "ymd String, " +
                    "TitleEn String, " +
                    "calculated_value Float64" +
                    ") ENGINE = MergeTree() " +
                    "ORDER BY ymd";
        
        try (Connection conn = DatabaseConfig.getConnection();
             Statement stmt = conn.createStatement()) {
            stmt.execute(sql);
            System.out.println("✓ 结果表创建完成: " + tableName);
        }
    }
    
    public static void main(String[] args) {
        // 🎯 在main函数中定义表名
        SOURCE_TABLE_NAME = "tmp.combined_all_data_for_bq";  // 可以根据需要修改
        
        long startTime = System.currentTimeMillis();
        
        try {
            System.out.println("=== 复杂月度因子计算程序开始 ===");
            System.out.println("数据源表: " + SOURCE_TABLE_NAME);
            System.out.println("结果表: " + RESULT_TABLE_NAME);
            System.out.println("计算时间范围: " + START_DATE + " 到 " + END_DATE);
            
            
            // 1. 创建结果表
            System.out.println("\n[步骤1] 创建结果表...");
            createResultTableIfNotExists(RESULT_TABLE_NAME);  // 改为调用本类的方法
            System.out.println("✓ 结果表创建完成: ods." + RESULT_TABLE_NAME);
            
            // 2. 获取复杂因子记录
            System.out.println("\n[步骤2] 读取复杂因子数据...");
            List<FactorDAO.FactorRecord> complexFactors = getComplexFactorRecords();
            System.out.println("✓ 共找到 " + complexFactors.size() + " 个复杂因子");
            
            if (complexFactors.isEmpty()) {
                System.out.println("⚠️ 没有找到包含'本期'和'上期'的因子，程序结束");
                return;
            }
            
            // 3. 处理每个复杂因子
            System.out.println("\n[步骤3] 开始计算复杂因子...");
            AtomicInteger completedCount = new AtomicInteger(0);
            
            for (FactorDAO.FactorRecord factor : complexFactors) {
                try {
                    processComplexFactor(factor);
                    int completed = completedCount.incrementAndGet();
                    System.out.println(String.format("进度: %d/%d 完成", completed, complexFactors.size()));
                } catch (Exception e) {
                    System.err.println("❌ 处理因子失败: " + factor.getTitleEn() + ", 错误: " + e.getMessage());
                    e.printStackTrace();
                }
            }
            
            long endTime = System.currentTimeMillis();
            System.out.println(String.format("\n=== 复杂因子计算完成，耗时: %.2f 秒 ===", 
                (endTime - startTime) / 1000.0));
                
        } catch (Exception e) {
            System.err.println("❌ 程序执行失败: " + e.getMessage());
            e.printStackTrace();
        }
    }
}