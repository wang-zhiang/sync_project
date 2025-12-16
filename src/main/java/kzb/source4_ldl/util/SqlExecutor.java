package kzb.source4_ldl.util;

import kzb.ClickHouseUtil;
import java.sql.SQLException;

public class SqlExecutor {
    private String connectionUrl;
    
    public SqlExecutor(String connectionUrl) {
        this.connectionUrl = connectionUrl;
    }
    
    public void executeSql(String sql, String description) {
        try {
            System.out.println("执行: " + description);
            System.out.println("SQL: " + sql);
            ClickHouseUtil.executeUpdate(connectionUrl, sql);
            System.out.println("成功: " + description);
        } catch (SQLException e) {
            System.err.println("失败: " + description + ", 错误: " + e.getMessage());
            throw new RuntimeException(e);
        }
    }
    
    public long getTableCount(String tableName) {
        try {
            String sql = "SELECT count(*) FROM " + tableName;
            return ClickHouseUtil.executeCountQuery(connectionUrl, sql);
        } catch (SQLException e) {
            System.err.println("获取表记录数失败: " + tableName + ", 错误: " + e.getMessage());
            return -1;
        }
    }
}