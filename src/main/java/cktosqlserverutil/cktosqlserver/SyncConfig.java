package cktosqlserverutil.cktosqlserver;

public class SyncConfig {
    // ClickHouse配置
    private String clickHouseUrl;
    private String clickHouseUser = "default";
    private String clickHousePassword = "smartpath";
    
    // SQL Server配置
    private String sqlServerUrl;
    private String sqlServerUser;
    private String sqlServerPassword;
    
    // 同步配置
    private String sourceTable;
    private String targetTable;
    private String whereCondition = "1=1";
    private int batchSize = 2000;            // 减小批次大小
    private int threadCount = 3;             // 减少线程数
    private int pageSize = 5000;             // 减小分页大小
    private int maxRetries = 5;              // 增加重试次数
    private long retryDelayMs = 2000;        // 增加重试延迟
    private String orderColumn; // 写死的排序字段
    
    // 构造函数
    public SyncConfig(String clickHouseUrl, String sqlServerUrl, 
                     String sqlServerUser, String sqlServerPassword) {
        this.clickHouseUrl = clickHouseUrl;
        this.sqlServerUrl = sqlServerUrl;
        this.sqlServerUser = sqlServerUser;
        this.sqlServerPassword = sqlServerPassword;
    }
    
    // 添加唯一列配置
    private String uniqueColumn;  // 唯一列名
    
    // Getters and Setters
    public String getClickHouseUrl() { return clickHouseUrl; }
    public String getClickHouseUser() { return clickHouseUser; }
    public String getClickHousePassword() { return clickHousePassword; }
    public String getSqlServerUrl() { return sqlServerUrl; }
    public String getSqlServerUser() { return sqlServerUser; }
    public String getSqlServerPassword() { return sqlServerPassword; }
    public String getSourceTable() { return sourceTable; }
    public void setSourceTable(String sourceTable) { this.sourceTable = sourceTable; }
    public String getTargetTable() { return targetTable != null ? targetTable : sourceTable; }
    public void setTargetTable(String targetTable) { this.targetTable = targetTable; }
    public String getWhereCondition() { return whereCondition; }
    public void setWhereCondition(String whereCondition) { this.whereCondition = whereCondition; }
    public int getBatchSize() { return batchSize; }
    public void setBatchSize(int batchSize) { this.batchSize = batchSize; }
    public int getThreadCount() { return threadCount; }
    public void setThreadCount(int threadCount) { this.threadCount = threadCount; }
    public int getPageSize() { return pageSize; }
    public void setPageSize(int pageSize) { this.pageSize = pageSize; }
    public int getMaxRetries() { return maxRetries; }
    public void setMaxRetries(int maxRetries) { this.maxRetries = maxRetries; }
    public long getRetryDelayMs() { return retryDelayMs; }
    public void setRetryDelayMs(long retryDelayMs) { this.retryDelayMs = retryDelayMs; }
    
    // 添加getter和setter
    public String getUniqueColumn() { return uniqueColumn; }
    public void setUniqueColumn(String uniqueColumn) { this.uniqueColumn = uniqueColumn; }
    public String getOrderColumn() {
        return orderColumn;
    }
    
    public void setOrderColumn(String orderColumn) {
        this.orderColumn = orderColumn;
    }
}