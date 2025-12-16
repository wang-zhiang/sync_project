package cktosqlserverutil.cktosqlserver;

import ru.yandex.clickhouse.ClickHouseConnection;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

public class ClickHouseToSqlServerSyncer {
    private SyncConfig config;
    private DatabaseManager dbManager;
    private ExecutorService executorService;
    private BlockingQueue<DataBatch> taskQueue;
    private String orderColumn; // 用于分页的排序字段
    
    public ClickHouseToSqlServerSyncer(SyncConfig config) {
        this.config = config;
        this.dbManager = new DatabaseManager(config);
        this.executorService = Executors.newFixedThreadPool(config.getThreadCount());
        this.taskQueue = new LinkedBlockingQueue<>(config.getThreadCount() * 2);
        // 使用配置中写死的排序字段
        this.orderColumn = config.getOrderColumn();
    }
    
    public void sync() {
        long startTime = System.currentTimeMillis();
        System.out.println("开始同步数据...");
        System.out.println("源表: " + config.getSourceTable());
        System.out.println("目标表: " + config.getTargetTable());
        System.out.println("筛选条件: " + config.getWhereCondition());
        System.out.println("线程数: " + config.getThreadCount());
        System.out.println("批次大小: " + config.getBatchSize());
        
        // 显示使用的排序字段
        if (orderColumn != null && !orderColumn.trim().isEmpty()) {
            System.out.println("使用指定的排序字段: " + orderColumn);
        } else {
            System.out.println("未指定排序字段，使用ROW_NUMBER分页");
        }
        
        try {
            // 1. 获取表结构
            List<ColumnInfo> columns = dbManager.getTableSchema(config.getSourceTable());
            System.out.println("表字段数: " + columns.size());
            
            // 2. 检查并创建目标表
            if (!dbManager.tableExists(config.getTargetTable())) {
                System.out.println("目标表不存在，正在创建...");
                dbManager.createTable(config.getTargetTable(), columns);
            } else {
                System.out.println("目标表已存在: " + config.getTargetTable());
            }
            
            // 3. 获取总记录数
            long totalCount = dbManager.getTotalCount(config.getSourceTable(), config.getWhereCondition());
            System.out.println("总记录数: " + totalCount);
            
            if (totalCount == 0) {
                System.out.println("没有数据需要同步");
                return;
            }
            
            // 4. 启动消费者线程
            List<SyncTask> tasks = new ArrayList<>();
            for (int i = 0; i < config.getThreadCount(); i++) {
                SyncTask task = new SyncTask(config, dbManager, taskQueue, columns);
                tasks.add(task);
                executorService.submit(task);
            }
            
            // 5. 生产数据
            produceData(columns, totalCount);
            
            // 6. 发送毒丸，通知线程退出
            for (int i = 0; i < config.getThreadCount(); i++) {
                taskQueue.put(DataBatch.poison());
            }
            
            // 7. 等待所有任务完成
            executorService.shutdown();
            if (!executorService.awaitTermination(2, TimeUnit.HOURS)) {
                System.err.println("同步超时，强制关闭线程池");
                executorService.shutdownNow();
            }
            
            long endTime = System.currentTimeMillis();
            long duration = endTime - startTime;
            System.out.println(String.format("同步完成！耗时: %.2f秒, 平均速度: %.0f条/秒", 
                duration / 1000.0, totalCount * 1000.0 / duration));
            
        } catch (Exception e) {
            System.err.println("同步失败: " + e.getMessage());
            e.printStackTrace();
        }
    }
    
    private void produceData(List<ColumnInfo> columns, long totalCount) throws Exception {
        System.out.println("开始读取数据，总记录数: " + totalCount);
        
        // 优先使用配置中指定的排序字段
        if (orderColumn != null && !orderColumn.trim().isEmpty()) {
            System.out.println("使用指定排序字段分页: " + orderColumn);
            produceDataByUniqueColumn(columns, orderColumn);
        } else {
            // 如果没有指定排序字段，检查配置中的唯一列
            String uniqueColumn = config.getUniqueColumn();
            if (uniqueColumn != null && !uniqueColumn.trim().isEmpty()) {
                produceDataByUniqueColumn(columns, uniqueColumn);
            } else {
                // 使用ROW_NUMBER分页
                produceDataByRowNumber(columns, totalCount);
            }
        }
    }
    
    private void produceDataByUniqueColumn(List<ColumnInfo> columns, String uniqueColumn) throws Exception {
        Object lastValue = null;
        int batchNumber = 0;
        int totalProcessed = 0;
        
        System.out.println("使用唯一列分页: " + uniqueColumn);
        
        while (true) {
            try {
                List<Map<String, Object>> pageData = dbManager.getDataByUniqueColumn(
                    config.getSourceTable(), uniqueColumn, config.getWhereCondition(),
                    columns, lastValue, config.getPageSize()
                );
                
                if (pageData.isEmpty()) {
                    break; // 没有更多数据
                }
                
                // 将页数据分批
                for (int i = 0; i < pageData.size(); i += config.getBatchSize()) {
                    int endIndex = Math.min(i + config.getBatchSize(), pageData.size());
                    List<Map<String, Object>> batchData = pageData.subList(i, endIndex);
                    
                    DataBatch batch = new DataBatch(++batchNumber, new ArrayList<>(batchData));
                    taskQueue.put(batch);
                }
                
                // 更新lastValue为当前页最后一条记录的唯一列值
                Map<String, Object> lastRow = pageData.get(pageData.size() - 1);
                lastValue = lastRow.get(uniqueColumn);
                
                totalProcessed += pageData.size();
                System.out.println(String.format("读取进度: 已处理 %d 条记录, 当前页记录数: %d, 最后值: %s", 
                    totalProcessed, pageData.size(), lastValue));
                    
            } catch (Exception e) {
                System.err.println("读取数据失败，最后值: " + lastValue + ", 错误: " + e.getMessage());
                
                // 简化的重试机制
                int retries = 0;
                boolean retrySuccess = false;
                
                while (retries < config.getMaxRetries()) {
                    try {
                        Thread.sleep(config.getRetryDelayMs() * (retries + 1));
                        
                        List<Map<String, Object>> pageData = dbManager.getDataByUniqueColumn(
                            config.getSourceTable(), uniqueColumn, config.getWhereCondition(),
                            columns, lastValue, config.getPageSize()
                        );
                        
                        if (pageData.isEmpty()) {
                            return; // 没有更多数据，结束
                        }
                        
                        for (int i = 0; i < pageData.size(); i += config.getBatchSize()) {
                            int endIndex = Math.min(i + config.getBatchSize(), pageData.size());
                            List<Map<String, Object>> batchData = pageData.subList(i, endIndex);
                            DataBatch batch = new DataBatch(++batchNumber, new ArrayList<>(batchData));
                            taskQueue.put(batch);
                        }
                        
                        Map<String, Object> lastRow = pageData.get(pageData.size() - 1);
                        lastValue = lastRow.get(uniqueColumn);
                        totalProcessed += pageData.size();
                        
                        System.out.println("重试成功，继续处理");
                        retrySuccess = true;
                        break;
                        
                    } catch (Exception retryEx) {
                        retries++;
                        System.err.println("第" + retries + "次重试失败: " + retryEx.getMessage());
                    }
                }
                
                if (!retrySuccess) {
                    System.err.println("重试次数耗尽，跳过当前位置");
                    return;
                }
            }
        }
        
        System.out.println("数据读取完成，总批次数: " + batchNumber + ", 总记录数: " + totalProcessed);
    }
    
    // 实现ROW_NUMBER分页方法
    private void produceDataByRowNumber(List<ColumnInfo> columns, long totalCount) throws Exception {
        int batchNumber = 0;
        int totalProcessed = 0;
        
        System.out.println("使用ROW_NUMBER分页");
        
        for (long offset = 0; offset < totalCount; offset += config.getPageSize()) {
            try {
                List<Map<String, Object>> pageData = dbManager.getDataByRowNumber(
                    config.getSourceTable(), config.getWhereCondition(),
                    columns, offset, config.getPageSize()
                );
                
                if (pageData.isEmpty()) {
                    break;
                }
                
                // 将页数据分批
                for (int i = 0; i < pageData.size(); i += config.getBatchSize()) {
                    int endIndex = Math.min(i + config.getBatchSize(), pageData.size());
                    List<Map<String, Object>> batchData = pageData.subList(i, endIndex);
                    
                    DataBatch batch = new DataBatch(++batchNumber, new ArrayList<>(batchData));
                    taskQueue.put(batch);
                }
                
                totalProcessed += pageData.size();
                System.out.println(String.format("ROW_NUMBER分页进度: 已处理 %d/%d 条记录", 
                    totalProcessed, totalCount));
                    
            } catch (Exception e) {
                System.err.println("ROW_NUMBER分页读取失败，偏移量: " + offset + ", 错误: " + e.getMessage());
                throw e;
            }
        }
        
        System.out.println("ROW_NUMBER分页完成，总批次数: " + batchNumber + ", 总记录数: " + totalProcessed);
    }
    
    // 检查是否是致命错误
    private boolean isFatalError(Exception e) {
        String message = e.getMessage().toLowerCase();
        return message.contains("table doesn't exist") || 
               message.contains("column doesn't exist") ||
               message.contains("syntax error") ||
               message.contains("access denied");
    }
    
    // 获取目标表记录数
    private long getTargetTableCount() throws SQLException {
        String sql = "SELECT COUNT(*) FROM " + config.getTargetTable();
        
        try (Connection conn = dbManager.getSqlServerConnection();
             Statement stmt = conn.createStatement()) {
            
            try (ResultSet rs = stmt.executeQuery(sql)) {
                if (rs.next()) {
                    return rs.getLong(1);
                }
                return 0;
            }
        }
    }
    
    // 数据完整性验证
    private void validateDataIntegrity() throws SQLException {
        // 比较源表和目标表的记录数
        long sourceCount = dbManager.getTotalCount(config.getSourceTable(), config.getWhereCondition());
        long targetCount = getTargetTableCount();
        
        System.out.println(String.format("数据完整性检查 - 源表: %d, 目标表: %d", sourceCount, targetCount));
        
        if (targetCount > sourceCount) {
            System.err.println("警告：目标表记录数超过源表，可能存在重复数据！");
            // 可以添加重复数据检测和清理逻辑
        } else if (targetCount < sourceCount) {
            System.err.println("警告：目标表记录数少于源表，可能存在数据丢失！");
        } else {
            System.out.println("数据完整性检查通过");
        }
    }
}