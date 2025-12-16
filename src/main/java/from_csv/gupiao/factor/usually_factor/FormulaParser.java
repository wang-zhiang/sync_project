package from_csv.gupiao.factor.usually_factor;

import java.text.DecimalFormat;
import java.time.LocalDate;
import java.util.*;
import java.text.DecimalFormat;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class FormulaParser {
    private static final double MISSING_VALUE = -99999999.0;
    
    // 🆕 添加日志缓存，避免重复输出
    private static final Set<String> loggedFormulas = new HashSet<>();
    
    /**
     * 解析公式并计算结果
     * @param formula 数学公式
     * @param indicatorValues 指标值映射
     * @param currentDate 当前计算日期（用于错误日志）
     * @return 计算结果，如果任一指标缺失则返回-99999999
     */
    public static double evaluateFormula(String formula, Map<String, Double> indicatorValues, LocalDate currentDate) {
        try {
            // 🆕 只为每个公式输出一次日志
            if (!loggedFormulas.contains(formula)) {
                System.out.println("\n🔍 开始解析公式: [" + formula + "]");
                
                // 显示提取的指标
                Set<String> indicators = extractIndicators(formula);
                System.out.println("📊 提取到的指标: " + indicators);
                
                // 显示指标值
                for (String indicator : indicators) {
                    Double value = indicatorValues.get(indicator);
                    System.out.println("  - " + indicator + " = " + value);
                }
                
                loggedFormulas.add(formula);
            }
            
            // 检查是否有指标值为-99999999或缺失
            for (String indicator : extractIndicators(formula)) {
                Double value = indicatorValues.get(indicator);
                if (value == null || Math.abs(value - MISSING_VALUE) < 0.001) {
                    return MISSING_VALUE;
                }
            }
            
            // 替换指标名为对应的数值
            String expression = replaceIndicatorsWithValues(formula, indicatorValues);
            
            // 🆕 显示替换后的表达式（只在首次解析时）
            if (!loggedFormulas.contains(formula + "_processed")) {
                System.out.println("🔄 替换后的表达式: [" + expression + "]");
                loggedFormulas.add(formula + "_processed");
            }
            
            // 计算表达式
            return evaluateExpression(expression);
        } catch (Exception e) {
            System.err.println("❌ 公式解析错误，程序终止！");
            System.err.println("📅 错误日期: " + currentDate);  // 🆕 添加日期信息
            System.err.println("错误公式: " + formula);
            System.err.println("错误信息: " + e.getMessage());
            
            // 🆕 显示更多调试信息
            try {
                Set<String> indicators = extractIndicators(formula);
                System.err.println("提取的指标: " + indicators);
                
                // 🆕 显示当前日期的指标值
                System.err.println("当前日期 [" + currentDate + "] 的指标值:");
                for (String indicator : indicators) {
                    Double value = indicatorValues.get(indicator);
                    System.err.println("  - " + indicator + " = " + value);
                }
                
                String expression = replaceIndicatorsWithValues(formula, indicatorValues);
                System.err.println("替换后表达式: [" + expression + "]");
            } catch (Exception debugE) {
                System.err.println("调试信息获取失败: " + debugE.getMessage());
            }
            
            e.printStackTrace();
            System.exit(1);
            return MISSING_VALUE;
        }
    }
    
    /**
     * 从公式中提取所有指标名
     */
    public static Set<String> extractIndicators(String formula) {
        Set<String> indicators = new HashSet<>();
        extractIndicatorsRecursive(formula, indicators);
        return indicators;
    }
    
    private static void extractIndicatorsRecursive(String formula, Set<String> indicators) {
        List<String> tokens = tokenizeFormula(formula);
        
        for (String token : tokens) {
            String cleanToken = token.trim();
            if (!cleanToken.isEmpty() && !isNumeric(cleanToken)) {
                if (cleanToken.startsWith("(") && cleanToken.endsWith(")")) {
                    // 检查括号内是否包含运算符
                    String innerFormula = cleanToken.substring(1, cleanToken.length() - 1);
                    if (containsTopLevelOperators(innerFormula)) {
                        // 括号内有运算符：递归分解
                        extractIndicatorsRecursive(innerFormula, indicators);
                    } else {
                        // 括号内无运算符：整个括号表达式是指标的一部分
                        // 这种情况不应该发生，因为tokenizeFormula不会单独提取无运算符的括号
                        indicators.add(cleanToken);
                    }
                } else {
                    // 普通指标：直接添加
                    indicators.add(cleanToken);
                }
            }
        }
    }
    
    // 检查是否包含顶层运算符（不在括号内的运算符）
    private static boolean containsTopLevelOperators(String text) {
        int parenthesesLevel = 0;
        for (int i = 0; i < text.length(); i++) {
            char c = text.charAt(i);
            if (c == '(') {
                parenthesesLevel++;
            } else if (c == ')') {
                parenthesesLevel--;
            } else if (parenthesesLevel == 0 && isOperator(c)) {
                return true;
            }
        }
        return false;
    }
    
    /**
     * 将公式分解为tokens，正确处理括号
     */
    private static List<String> tokenizeFormula(String formula) {
        List<String> tokens = new ArrayList<>();
        StringBuilder currentToken = new StringBuilder();
        int parenthesesLevel = 0;
        
        for (int i = 0; i < formula.length(); i++) {
            char c = formula.charAt(i);
            
            if (c == '(') {
                parenthesesLevel++;
                currentToken.append(c);  // 直接添加到当前token，不分割
            } else if (c == ')') {
                currentToken.append(c);
                parenthesesLevel--;
                // 移除括号闭合时的特殊处理
            } else if (parenthesesLevel == 0 && isOperator(c)) {
                // 只有在括号外才按运算符分割
                String token = currentToken.toString().trim();
                if (!token.isEmpty()) {
                    tokens.add(token);
                }
                currentToken = new StringBuilder();
            } else {
                currentToken.append(c);
            }
        }
        
        // 添加最后一个token
        if (currentToken.length() > 0) {
            String token = currentToken.toString().trim();
            if (!token.isEmpty()) {
                tokens.add(token);
            }
        }
        
        return tokens;
    }
    
    /**
     * 判断字符是否为运算符
     */
    private static boolean isOperator(char c) {
        return c == '+' || c == '-' || c == '*' || c == '/' || c == '÷' || c == '–';
    }
    
    /**
     * 将公式中的指标名替换为对应的数值
     */

    private static String replaceIndicatorsWithValues(String formula, Map<String, Double> indicatorValues) {
        String result = formula;
        
        // 🆕 首先统一运算符 - 将长横线替换为普通减号
        result = result.replace("–", "-");
        result = result.replace("÷", "/");
        
        // 按指标名长度降序排序，避免短名称替换长名称的一部分
        List<String> sortedIndicators = new ArrayList<>(indicatorValues.keySet());
        sortedIndicators.sort((a, b) -> Integer.compare(b.length(), a.length()));
        
        for (String indicator : sortedIndicators) {
            Double value = indicatorValues.get(indicator);
            if (value != null) {
                // 使用正则表达式确保完全匹配指标名
                String escapedIndicator = Pattern.quote(indicator);
                result = result.replaceAll(escapedIndicator, String.valueOf(value));
            }
        }
        
        return result;
    }
    
    /**
     * 计算数学表达式
     */
    private static double evaluateExpression(String expression) {
        return new ExpressionEvaluator().evaluate(expression);
    }
    
    private static boolean isNumeric(String str) {
        try {
            Double.parseDouble(str);
            return true;
        } catch (NumberFormatException e) {
            return false;
        }
    }
    
    /**
     * 表达式计算器 - 支持括号和四则运算
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
                        // 🔧 修复：遇到除零时返回MISSING_VALUE，而不是抛出异常
                        return MISSING_VALUE; // -99999999.0
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