package sqlservertomysql.config;

import java.util.Properties;

/**
 * 数据库配置类
 */
public class DatabaseConfig {
    // SQL Server 配置
    private String sqlServerUrl;
    private String sqlServerUsername;
    private String sqlServerPassword;
    
    // MySQL 配置
    private String mysqlUrl;
    private String mysqlUsername;
    private String mysqlPassword;
    
    // 同步配置 - 不设置默认值，由调用方决定
    private int pageSize;     // 分页大小
    private int threadCount;  // 线程数
    private int batchSize;    // 批次大小 - 新增
    private String syncCondition;
    
    public DatabaseConfig() {
        // 只设置数据库连接的默认配置
        this.sqlServerUrl = "jdbc:sqlserver://192.168.4.219;databaseName=datasystem";
        this.sqlServerUsername = "sa";
        this.sqlServerPassword = "smartpthdata";
        
        this.mysqlUrl = "jdbc:mysql://192.168.6.101:3306/datasystem?useSSL=false&serverTimezone=UTC";
        this.mysqlUsername = "root";
        this.mysqlPassword = "smartpath";
        
        // 同步参数不设置默认值，强制调用方明确指定
    }
    
    // Getters and Setters
    public String getSqlServerUrl() { return sqlServerUrl; }
    public void setSqlServerUrl(String sqlServerUrl) { this.sqlServerUrl = sqlServerUrl; }
    
    public String getSqlServerUsername() { return sqlServerUsername; }
    public void setSqlServerUsername(String sqlServerUsername) { this.sqlServerUsername = sqlServerUsername; }
    
    public String getSqlServerPassword() { return sqlServerPassword; }
    public void setSqlServerPassword(String sqlServerPassword) { this.sqlServerPassword = sqlServerPassword; }
    
    public String getMysqlUrl() { return mysqlUrl; }
    public void setMysqlUrl(String mysqlUrl) { this.mysqlUrl = mysqlUrl; }
    
    public String getMysqlUsername() { return mysqlUsername; }
    public void setMysqlUsername(String mysqlUsername) { this.mysqlUsername = mysqlUsername; }
    
    public String getMysqlPassword() { return mysqlPassword; }
    public void setMysqlPassword(String mysqlPassword) { this.mysqlPassword = mysqlPassword; }
    
    public int getPageSize() { return pageSize; }
    public void setPageSize(int pageSize) { this.pageSize = pageSize; }
    
    public int getThreadCount() { return threadCount; }
    public void setThreadCount(int threadCount) { this.threadCount = threadCount; }
    
    public String getSyncCondition() { return syncCondition; }
    public void setSyncCondition(String syncCondition) { this.syncCondition = syncCondition; }
    
    public int getBatchSize() { return batchSize; }
    public void setBatchSize(int batchSize) { this.batchSize = batchSize; }
}