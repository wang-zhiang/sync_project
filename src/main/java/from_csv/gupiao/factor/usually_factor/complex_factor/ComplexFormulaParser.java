package from_csv.gupiao.factor.usually_factor.complex_factor;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.text.DecimalFormat;

/**
 * 复杂公式解析器 - 优化版本，避免重复日志
 */
public class ComplexFormulaParser {
    // 正则表达式模式
    private static final Pattern INDICATOR_PATTERN = Pattern.compile("^(.+?)\\s*\\(本期\\)");
    
    // 缓存，避免重复解析
    private static final Map<String, String> indicatorCache = new HashMap<>();
    
    /**
     * 从公式中提取基础指标名称（优化版本）
     */
    public static String extractBaseIndicator(String formula) {
        if (formula == null || formula.trim().isEmpty()) {
            return null;
        }
        
        // 检查缓存
        if (indicatorCache.containsKey(formula)) {
            return indicatorCache.get(formula);
        }
        
        // 只在第一次解析时输出日志
        System.out.println("解析公式: " + formula);
        
        Matcher matcher = INDICATOR_PATTERN.matcher(formula.trim());
        if (matcher.find()) {
            String indicator = matcher.group(1).trim();
            System.out.println("✅ 提取指标: [" + indicator + "]");
            
            indicatorCache.put(formula, indicator);
            return indicator;
        }
        
        System.err.println("❌ 无法提取指标: " + formula);
        indicatorCache.put(formula, null);
        return null;
    }
    
    /**
     * 检查是否为复杂公式
     */
    public static boolean isComplexFormula(String formula) {
        return formula != null && formula.contains("(本期)") && formula.contains("(上期)");
    }
    
    /**
     * 替换公式中的(本期)和(上期)为实际数值
     */
    public static String replacePeriodsWithValues(String formula, String baseIndicator, 
                                                 double currentValue, double previousValue) {
        if (formula == null || baseIndicator == null) {
            return formula;
        }
        
        // 🔧 修复科学计数法问题：使用DecimalFormat确保数字以普通格式显示
        DecimalFormat df = new DecimalFormat("#");
        df.setMaximumFractionDigits(10);
        df.setGroupingUsed(false);
        
        String currentValueStr = df.format(currentValue);
        String previousValueStr = df.format(previousValue);
        
        return formula
            .replace(baseIndicator + " (本期)", currentValueStr)
            .replace(baseIndicator + " (上期)", previousValueStr)
            .replace(baseIndicator + "(本期)", currentValueStr)
            .replace(baseIndicator + "(上期)", previousValueStr);
    }
    
    /**
     * 计算简单数学表达式
     */
    public static double evaluateExpression(String expression) {
        try {
            // 移除空格
            expression = expression.replaceAll("\\s+", "");
            
            // 🔧 关键修复：将中文符号转换为英文符号
            expression = expression.replace("÷", "/");
            expression = expression.replace("–", "-");
            expression = expression.replace("−", "-");
            
            // 🆕 新增：去掉中文描述部分，只保留数学表达式
            expression = expression.replaceAll("[\u4e00-\u9fa5]+", "");
            expression = expression.trim();
            
            // 使用更强大的表达式计算器
            return new ExpressionEvaluator().evaluate(expression);
            
        } catch (Exception e) {
            // 🔧 修复：不再返回-99999999，而是抛出异常让上层处理
            System.err.println("❌ 复杂因子表达式计算错误，程序终止！");
            System.err.println("错误表达式: " + expression);
            System.err.println("错误信息: " + e.getMessage());
            throw new RuntimeException("复杂因子表达式解析失败: " + expression, e);
        }
    }
    
    /**
     * 表达式计算器 - 支持括号和四则运算（从FormulaParser复制）
     */
    private static class ExpressionEvaluator {
        private int pos = -1;
        private int ch;
        private String expression;
        
        public double evaluate(String expression) {
            this.expression = expression.replaceAll("\\s+", "");
            this.pos = -1;
            nextChar();
            double result = parseExpression();
            if (pos < this.expression.length()) {
                throw new RuntimeException("表达式解析错误");
            }
            return result;
        }
        
        private void nextChar() {
            ch = (++pos < expression.length()) ? expression.charAt(pos) : -1;
        }
        
        private boolean eat(int charToEat) {
            while (ch == ' ') nextChar();
            if (ch == charToEat) {
                nextChar();
                return true;
            }
            return false;
        }
        
        private double parseExpression() {
            double x = parseTerm();
            for (;;) {
                if (eat('+')) x += parseTerm();
                else if (eat('-')) x -= parseTerm();
                else return x;
            }
        }
        
        private double parseTerm() {
            double x = parseFactor();
            for (;;) {
                if (eat('*')) x *= parseFactor();
                else if (eat('/')) {
                    double divisor = parseFactor();
                    if (Math.abs(divisor) < 1e-10) {
                        throw new RuntimeException("除零错误");
                    }
                    x /= divisor;
                }
                else return x;
            }
        }
        
        private double parseFactor() {
            if (eat('+')) return parseFactor();
            if (eat('-')) return -parseFactor();
            
            double x;
            int startPos = this.pos;
            if (eat('(')) {
                x = parseExpression();
                eat(')');
            } else if ((ch >= '0' && ch <= '9') || ch == '.') {
                while ((ch >= '0' && ch <= '9') || ch == '.') nextChar();
                x = Double.parseDouble(expression.substring(startPos, this.pos));
            } else {
                throw new RuntimeException("意外字符: " + (char)ch);
            }
            
            return x;
        }
    }
}