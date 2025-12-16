package from_csv.gupiao.factor.usually_factor.complex_month_factor;

import java.time.LocalDate;
import java.util.Map;

/**
 * 时间序列计算器 - 处理上期值查找逻辑
 */
public class TimeSeriesCalculator {
    
    private final ComplexFactorDAO dao;
    private final int defaultLookbackDays;
    
    public TimeSeriesCalculator(ComplexFactorDAO dao, int defaultLookbackDays) {
        this.dao = dao;
        this.defaultLookbackDays = defaultLookbackDays;
    }
    
    /**
     * 查找指定日期之前的第一个有效值（上期值）
     * @param indicator 指标名称
     * @param currentDate 当前日期
     * @param maxLookbackDays 最大回溯天数（防止无限回溯）
     * @return 上期值，如果找不到返回null
     */
    public Double findPreviousValidValue(String indicator, LocalDate currentDate, int maxLookbackDays) {
        LocalDate searchDate = currentDate.minusDays(1);
        LocalDate earliestDate = currentDate.minusDays(maxLookbackDays);
        
        // 优化：批量获取一段时间内的数据
        Map<LocalDate, Double> historicalData = dao.getIndicatorValuesInRange(
            indicator, earliestDate, currentDate.minusDays(1));
        
        // 从最近的日期开始查找
        while (!searchDate.isBefore(earliestDate)) {
            Double value = historicalData.get(searchDate);
            if (value != null && value != -99999999.0) {
                return value;
            }
            searchDate = searchDate.minusDays(1);
        }
        
        return null;
    }
    
    /**
     * 使用默认的最大回溯天数查找上期值
     */
    public Double findPreviousValidValue(String indicator, LocalDate currentDate) {
        return findPreviousValidValue(indicator, currentDate, defaultLookbackDays);
    }
    
    /**
     * 计算复杂公式的结果
     * @param formula 包含(本期)和(上期)的公式
     * @param currentDate 当前计算日期
     * @return 计算结果
     */
    public double calculateComplexFormula(String formula, LocalDate currentDate) {
        // 1. 提取基础指标
        String baseIndicator = ComplexFormulaParser.extractBaseIndicator(formula);
        if (baseIndicator == null) {
            System.err.println("无法从公式中提取指标名: " + formula);
            return -99999999.0;
        }
        
        // 2. 获取本期值
        Double currentValue = dao.getIndicatorValue(baseIndicator, currentDate);
        if (currentValue == null || currentValue == -99999999.0) {
            return -99999999.0;
        }
        
        // 3. 查找上期值
        Double previousValue = findPreviousValidValue(baseIndicator, currentDate);
        if (previousValue == null || previousValue == -99999999.0) {
            return -99999999.0;
        }
        
        // 4. 替换公式中的期间值
        String calculableFormula = ComplexFormulaParser.replacePeriodsWithValues(
            formula, baseIndicator, currentValue, previousValue);
        
        // 5. 计算结果
        double result = ComplexFormulaParser.evaluateExpression(calculableFormula);
        
        // 调试输出
        if (result != -99999999.0) {
            System.out.println(String.format("[%s] %s: 本期=%.2f, 上期=%.2f, 结果=%.6f", 
                currentDate, baseIndicator, currentValue, previousValue, result));
        }
        
        return result;
    }
    
    /**
     * 获取默认回溯天数
     */
    public int getDefaultLookbackDays() {
        return defaultLookbackDays;
    }
}