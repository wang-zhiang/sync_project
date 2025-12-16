package from_csv.gupiao.factor.usually_factor.complex_month_factor;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDate;
import java.util.HashMap;
import java.util.Map;

/**
 * 复杂因子数据访问对象
 */
public class ComplexFactorDAO {
    
    private final String sourceTableName;
    private final String resultTableName;
    
    public ComplexFactorDAO(String sourceTableName, String resultTableName) {
        this.sourceTableName = sourceTableName;
        this.resultTableName = resultTableName;
    }
    
    /**
     * 检查指标是否存在于数据库中
     * @param indicator 指标名称
     * @return 如果指标存在返回true，否则返回false
     */
    public boolean checkIndicatorExists(String indicator) {
        String sql = "SELECT COUNT(*) as count FROM " + sourceTableName + " WHERE indicator = ? LIMIT 1";
        
        try (Connection conn = ComplexDatabaseConfig.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {

            stmt.setString(1, indicator);

            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    int count = rs.getInt("count");
                    boolean exists = count > 0;

                    if (exists) {
                        System.out.println("✅ 指标存在于数据库: [" + indicator + "], 记录数: " + count);
                    } else {
                        System.err.println("❌ 指标不存在于数据库: [" + indicator + "]");
                    }

                    return exists;
                }
            }
        } catch (SQLException e) {
            System.err.println("检查指标存在性失败: " + indicator + ", 错误: " + e.getMessage());
            return false;
        }

        return false;
    }

    /**
     * 获取指定指标在指定日期的值
     */
    public Double getIndicatorValue(String indicator, LocalDate date) {
        String sql = "SELECT value FROM " + sourceTableName + " WHERE ymd = ? AND indicator = ?";

        try (Connection conn = ComplexDatabaseConfig.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {

            stmt.setString(1, date.toString());
            stmt.setString(2, indicator);

            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    double value = rs.getDouble("value");
                    return rs.wasNull() ? null : value;
                }
            }
        } catch (SQLException e) {
            System.err.println("查询指标值失败: " + indicator + ", 日期: " + date + ", 错误: " + e.getMessage());
        }

        return null;
    }

    /**
     * 获取指定指标在指定日期范围内的所有数据
     * 用于优化查询性能
     */
    public Map<LocalDate, Double> getIndicatorValuesInRange(String indicator, LocalDate startDate, LocalDate endDate) {
        Map<LocalDate, Double> result = new HashMap<>();
        String sql = "SELECT ymd, value FROM " + sourceTableName + " " +
                    "WHERE indicator = ? AND ymd >= ? AND ymd <= ? " +
                    "ORDER BY ymd DESC";

        try (Connection conn = ComplexDatabaseConfig.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {

            stmt.setString(1, indicator);
            stmt.setString(2, startDate.toString());
            stmt.setString(3, endDate.toString());

            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    LocalDate date = LocalDate.parse(rs.getString("ymd"));
                    double value = rs.getDouble("value");
                    if (!rs.wasNull() && value != -99999999.0) {
                        result.put(date, value);
                    }
                }
            }
        } catch (SQLException e) {
            System.err.println("批量查询指标值失败: " + indicator + ", 错误: " + e.getMessage());
        }

        return result;
    }

    /**
     * 插入计算结果
     */
    public void insertResult(String factorName, LocalDate date, double value) {
        String sql = "INSERT INTO " + resultTableName + " (factor_name, ymd, value) VALUES (?, ?, ?)";

        try (Connection conn = ComplexDatabaseConfig.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            
            stmt.setString(1, factorName);
            stmt.setString(2, date.toString());
            stmt.setDouble(3, value);
            
            stmt.executeUpdate();
        } catch (SQLException e) {
            System.err.println("插入结果失败: " + factorName + ", 日期: " + date + ", 错误: " + e.getMessage());
        }
    }
    
    /**
     * 批量插入计算结果
     */
    public void batchInsertResults(String factorName, Map<LocalDate, Double> results) {
        String sql = "INSERT INTO " + resultTableName + " (factor_name, ymd, value) VALUES (?, ?, ?)";
        
        try (Connection conn = ComplexDatabaseConfig.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            
            for (Map.Entry<LocalDate, Double> entry : results.entrySet()) {
                stmt.setString(1, factorName);
                stmt.setString(2, entry.getKey().toString());
                stmt.setDouble(3, entry.getValue());
                stmt.addBatch();
            }
            
            stmt.executeBatch();
            System.out.println("批量插入完成: " + factorName + ", 记录数: " + results.size());
        } catch (SQLException e) {
            System.err.println("批量插入失败: " + factorName + ", 错误: " + e.getMessage());
        }
    }
    
    /**
     * 获取源表名
     */
    public String getSourceTableName() {
        return sourceTableName;
    }
    
    /**
     * 获取结果表名
     */
    public String getResultTableName() {
        return resultTableName;
    }
}