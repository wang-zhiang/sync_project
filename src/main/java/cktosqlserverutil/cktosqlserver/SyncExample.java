package cktosqlserverutil.cktosqlserver;

public class SyncExample {

    //我有个好方法，分批读，分批连接ck  ,这个批次处理完，断开连接，然后建立连接

    public static void main(String[] args) {
        // 配置连接信息
        String clickHouseUrl = "jdbc:clickhouse://192.168.5.104:8123/dwd";
        String sqlServerUrl ="jdbc:sqlserver://192.168.4.51;DatabaseName=Taobao_trading";
        String sqlServerUser = "sa";
        String sqlServerPassword = "smartpathdata";
        
        // 创建配置
        SyncConfig config = new SyncConfig(clickHouseUrl, sqlServerUrl, sqlServerUser, sqlServerPassword);
        
        // 设置同步参数
        config.setSourceTable("dwd.taobaoduibi_res_2508");           // 源表名
        config.setTargetTable("taobaoduibi_res_2508_new");           // 目标表名
        config.setWhereCondition(" 1=  1");                         // 筛选条件
        config.setBatchSize(30000);                                  // 批次大小
        config.setThreadCount(10);                                   // 线程数
        config.setPageSize(1000000);                                 // 分页大小
        
        // 设置排序字段（如果需要的话）
        config.setOrderColumn("good_id"); // 或者其他合适的排序字段
        
        // 执行同步
        ClickHouseToSqlServerSyncer syncer = new ClickHouseToSqlServerSyncer(config);
        syncer.sync();
    }
}