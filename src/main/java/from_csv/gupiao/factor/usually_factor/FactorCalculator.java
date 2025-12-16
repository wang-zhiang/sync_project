package from_csv.gupiao.factor.usually_factor;

import java.sql.SQLException;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
/*
* Australia: Exports: FOB Value（本月）÷ Australia: Exports: FOB Value（上年同月） 计算月度同比
UK: Exports of Total Trade: SA（本月）÷ UK: Exports of Total Trade: SA（上年同月） 计算月度同比
Australia: Imports: Customs Value（本月）÷ Australia: Imports: Customs Value（上年同月） 计算月度同比
UK: Imports of Total Trade: Current Prices: SA（本月）÷ UK: Imports of Total Trade: Current Prices: SA（上年同月） 计算月度同比

计算一般的加减乘除，不包含本期本月的数据

* */




public class FactorCalculator {
    
    // TODO: 请填入结果表名
    private static final String RESULT_TABLE_NAME = "ceshi_20251012";
    
    private static final LocalDate START_DATE = LocalDate.of(1928, 1, 1);
    //private static final LocalDate START_DATE = LocalDate.of(2025, 1, 1);
    private static final LocalDate END_DATE = LocalDate.of(2025, 12, 31);
    private static final int BATCH_SIZE = 5000; // 增加批量大小
    private static final int PROGRESS_INTERVAL = 365; // 每年显示一次进度
    
    /**
     * Java 8兼容的字符串重复方法
     */
    private static String repeat(String str, int count) {
        if (count <= 0) return "";
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < count; i++) {
            sb.append(str);
        }
        return sb.toString();
    }
    
    public static void main(String[] args) {
        long startTime = System.currentTimeMillis();
        
        try {
            System.out.println("=== 开始因子计算程序 ===");
            System.out.println("计算时间范围: " + START_DATE + " 到 " + END_DATE);
            
            // 1. 创建结果表
            System.out.println("\n[步骤1] 创建结果表...");
            FactorDAO.createResultTableIfNotExists(RESULT_TABLE_NAME);
            System.out.println("✓ 结果表创建完成: ods." + RESULT_TABLE_NAME);
            
            // 2. 获取所有factor记录
            System.out.println("\n[步骤2] 读取factor数据...");
            List<FactorDAO.FactorRecord> factorRecords = FactorDAO.getFactorRecords();
            System.out.println("✓ 共找到 " + factorRecords.size() + " 个因子");
            
            if (factorRecords.isEmpty()) {
                System.out.println("⚠️ 没有找到需要处理的因子，程序结束");
                return;
            }
            
            // 3. 处理每个因子
            System.out.println("\n[步骤3] 开始处理因子...");
            AtomicInteger processedCount = new AtomicInteger(0);
            
            for (FactorDAO.FactorRecord factor : factorRecords) {
                try {
                    System.out.println("\n" + repeat("=", 80));
                    System.out.println("🔄 开始处理因子 [" + (processedCount.get() + 1) + "/" + factorRecords.size() + "]: " + factor.getTitleEn());
                    System.out.println("📋 原始公式: " + factor.getFormula());
                    
                    long factorStartTime = System.currentTimeMillis();
                    processFactor(factor);
                    long factorEndTime = System.currentTimeMillis();
                    
                    int current = processedCount.incrementAndGet();
                    System.out.println("✅ 因子处理完成，耗时: " + (factorEndTime - factorStartTime) + "ms");
                    System.out.println("📊 总体进度: " + current + "/" + factorRecords.size() + " (" + String.format("%.1f", current * 100.0 / factorRecords.size()) + "%)");
                    
                } catch (Exception e) {
                    System.err.println("❌ 处理因子失败: " + factor.getTitleEn());
                    System.err.println("   错误信息: " + e.getMessage());
                    e.printStackTrace();
                }
            }
            
            long endTime = System.currentTimeMillis();
            System.out.println("\n" + repeat("=", 80));
            System.out.println("🎉 所有因子计算完成！");
            System.out.println("📈 总耗时: " + (endTime - startTime) / 1000.0 + " 秒");
            System.out.println("📊 平均每个因子耗时: " + (endTime - startTime) / factorRecords.size() / 1000.0 + " 秒");
            
        } catch (Exception e) {
            System.err.println("❌ 程序执行失败: " + e.getMessage());
            e.printStackTrace();
        }
    }
    
    /**
     * 处理单个因子
     */
    private static void processFactor(FactorDAO.FactorRecord factor) throws SQLException {
        Set<String> indicators = FormulaParser.extractIndicators(factor.getFormula());
        System.out.println("🔍 解析出的指标数量: " + indicators.size());
        
        // 添加详细的指标输出
        System.out.println("📋 解析出的具体指标:");
        int index = 1;
        for (String indicator : indicators) {
            System.out.println("   [" + index + "] " + indicator);
            index++;
        }
        
        if (indicators.isEmpty()) {
            System.out.println("⚠️ 警告: 公式中未找到有效指标，跳过处理");
            return;
        }
        
        // 🚀 关键优化：一次性获取所有数据
        System.out.println("📊 开始获取所有历史数据...");
        Map<LocalDate, Map<String, Double>> allData = FactorDAO.getAllIndicatorValues(indicators);
        System.out.println("✅ 数据获取完成，开始计算...");
        
        List<FactorDAO.CalculationResult> results = new ArrayList<FactorDAO.CalculationResult>();
        LocalDate currentDate = START_DATE;
        int dayCount = 0;
        int totalDays = (int) (END_DATE.toEpochDay() - START_DATE.toEpochDay() + 1);
        
        long calculationStart = System.nanoTime();
        
        while (!currentDate.isAfter(END_DATE)) {
            Map<String, Double> indicatorValues = allData.getOrDefault(currentDate, new HashMap<String, Double>());
            
            double result;
            if (indicatorValues.size() != indicators.size() || 
                indicatorValues.values().stream().anyMatch(v -> v == -99999999.0)) {
                result = -99999999.0;
            } else {
                result = FormulaParser.evaluateFormula(factor.getFormula(), indicatorValues, currentDate);
            }
            
            results.add(new FactorDAO.CalculationResult(currentDate, factor.getTitleEn(), result));
            
            // 显示进度（每年显示一次）
            if (dayCount % PROGRESS_INTERVAL == 0) {
                double progress = dayCount * 100.0 / totalDays;
                System.out.println(String.format("📅 %s: 进度=%.1f%%", 
                    currentDate.format(DateTimeFormatter.ISO_LOCAL_DATE), progress));
            }
            
            // 批量插入
            if (results.size() >= BATCH_SIZE) {
                FactorDAO.insertResults(RESULT_TABLE_NAME, results);
                System.out.println("💾 批量插入 " + results.size() + " 条记录");
                results.clear();
            }
            
            currentDate = currentDate.plusDays(1);
            dayCount++;
        }
        
        // 插入剩余结果
        if (!results.isEmpty()) {
            FactorDAO.insertResults(RESULT_TABLE_NAME, results);
            System.out.println("💾 最终插入 " + results.size() + " 条记录");
        }
        
        long calculationTime = (System.nanoTime() - calculationStart) / 1_000_000;
        System.out.println(String.format("⏱️ 计算完成，总耗时：%dms", calculationTime));
    }
}