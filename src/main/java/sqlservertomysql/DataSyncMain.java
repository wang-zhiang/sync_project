package sqlservertomysql;

import sqlservertomysql.config.DatabaseConfig;
import sqlservertomysql.service.DataSyncService;

/**
 * 数据同步主程序
 *
 * 数据太慢了，肯你大概还是得用seatunnel  2.3.1 的应该可以试试呗
 * 可以参考下ck导mysql的，这个就很快
 *
 */
public class DataSyncMain {
    public static void main(String[] args) {
        try {
            // 加载数据库驱动
            Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
            Class.forName("com.mysql.cj.jdbc.Driver");
            
            // 创建配置
            DatabaseConfig config = new DatabaseConfig();
            
            // 统一配置所有参数
            config.setPageSize(100000);        // 改为1000条/页，便于观察日志
            config.setThreadCount(6);        // 减少线程数，避免数据库压力过大
            config.setBatchSize(5000);        // 相应调整批次大小
            config.setSyncCondition("1 = 1"); // 同步条件
            
            // ========== 表名配置区域 ==========
            // 请在这里配置要同步的表名
            String sqlServerTableName = "amplify_avene_massskincare_ES";  // SQL Server源表名
            String mysqlTableName = "amplify_avene_massskincare_ES_ceshi";       // MySQL目标表名（可以不同）
            
            // 创建同步服务
            DataSyncService syncService = new DataSyncService(config);
            
            // 开始同步
            System.out.println("=== 开始同步表 ===");
            System.out.println("源表（SQL Server）: " + sqlServerTableName);
            System.out.println("目标表（MySQL）: " + mysqlTableName);
            
            syncService.syncTable(sqlServerTableName, mysqlTableName);
            
            System.out.println("=== 表同步完成 ===");
            System.out.println("同步成功！");
            
        } catch (Exception e) {
            System.err.println("同步过程中发生错误: " + e.getMessage());
            e.printStackTrace();
        }
    }
}