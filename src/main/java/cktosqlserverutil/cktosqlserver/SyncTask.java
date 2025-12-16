package cktosqlserverutil.cktosqlserver;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;

public class SyncTask implements Runnable {
    private SyncConfig config;
    private DatabaseManager dbManager;
    private BlockingQueue<DataBatch> taskQueue;
    private List<ColumnInfo> columns;
    private String insertSql;
    private volatile boolean running = true;
    
    public SyncTask(SyncConfig config, DatabaseManager dbManager, 
                   BlockingQueue<DataBatch> taskQueue, List<ColumnInfo> columns) {
        this.config = config;
        this.dbManager = dbManager;
        this.taskQueue = taskQueue;
        this.columns = columns;
        this.insertSql = buildInsertSql();
    }
    
    private String buildInsertSql() {
        StringBuilder sql = new StringBuilder();
        sql.append("INSERT INTO ").append(config.getTargetTable()).append(" (");
        
        for (int i = 0; i < columns.size(); i++) {
            if (i > 0) sql.append(", ");
            sql.append("[").append(columns.get(i).getName()).append("]");
        }
        
        sql.append(") VALUES (");
        for (int i = 0; i < columns.size(); i++) {
            if (i > 0) sql.append(", ");
            sql.append("?");
        }
        sql.append(")");
        
        return sql.toString();
    }
    
    @Override
    public void run() {
        String threadName = Thread.currentThread().getName();
        System.out.println("同步线程启动: " + threadName);
        
        while (running) {
            try {
                DataBatch batch = taskQueue.take();
                if (batch.isPoison()) {
                    break; // 毒丸，退出循环
                }
                
                processBatch(batch, threadName);
                
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            } catch (Exception e) {
                System.err.println("线程 " + threadName + " 处理异常: " + e.getMessage());
                e.printStackTrace();
            }
        }
        
        System.out.println("同步线程结束: " + threadName);
    }
    
    private void processBatch(DataBatch batch, String threadName) {
        int retries = 0;
        while (retries <= config.getMaxRetries()) {
            try {
                // 在重试前检查数据是否已存在（如果有唯一键）
                if (retries > 0 && hasUniqueKey()) {
                    batch = filterExistingData(batch);
                    if (batch.getData().isEmpty()) {
                        System.out.println(String.format("[%s] 批次 %d 数据已存在，跳过", 
                            threadName, batch.getBatchNumber()));
                        return;
                    }
                }
                
                insertBatchWithTransaction(batch.getData(), threadName);
                System.out.println(String.format("[%s] 批次 %d 处理成功，记录数: %d", 
                    threadName, batch.getBatchNumber(), batch.getData().size()));
                return; // 成功，退出重试循环
                
            } catch (Exception e) {
                retries++;
                // 分析错误类型，决定是否重试
                if (isDuplicateKeyError(e) && retries == 1) {
                    System.out.println(String.format("[%s] 批次 %d 检测到重复键，尝试过滤重复数据", 
                        threadName, batch.getBatchNumber()));
                    continue; // 继续重试，但会过滤已存在的数据
                }
                System.err.println(String.format("[%s] 批次 %d 处理失败 (第%d次重试): %s", 
                    threadName, batch.getBatchNumber(), retries, e.getMessage()));
                
                if (retries <= config.getMaxRetries()) {
                    try {
                        Thread.sleep(config.getRetryDelayMs() * retries); // 指数退避
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        throw new RuntimeException(String.format("同步任务被中断，批次 %d 处理失败。断点信息：%s", 
                            batch.getBatchNumber(), getBreakpointInfo(batch)), ie);
                    }
                } else {
                    // 重试次数耗尽，抛出异常而不是跳过
                    throw new RuntimeException(String.format(
                        "批次 %d 重试 %d 次后仍然失败，同步终止。断点信息：%s。原始错误：%s", 
                        batch.getBatchNumber(), config.getMaxRetries(), getBreakpointInfo(batch), e.getMessage()), e);
                }
            }
        }
    }
    
    // 新增方法：获取断点信息
    private String getBreakpointInfo(DataBatch batch) {
        if (batch == null || batch.getData() == null || batch.getData().isEmpty()) {
            return "无数据信息";
        }
        
        String uniqueColumn = config.getUniqueColumn();
        if (uniqueColumn == null || uniqueColumn.trim().isEmpty()) {
            return String.format("批次号：%d，记录数：%d（无唯一列配置）", 
                batch.getBatchNumber(), batch.getData().size());
        }
        
        // 获取第一条和最后一条记录的唯一列值
        List<Map<String, Object>> data = batch.getData();
        Object firstValue = data.get(0).get(uniqueColumn);
        Object lastValue = data.get(data.size() - 1).get(uniqueColumn);
        
        return String.format("批次号：%d，记录数：%d，唯一列 '%s' 范围：%s ~ %s", 
            batch.getBatchNumber(), data.size(), uniqueColumn, firstValue, lastValue);
    }
    
    // 检查是否有唯一键
    private boolean hasUniqueKey() {
        String uniqueColumn = config.getUniqueColumn();
        return uniqueColumn != null && !uniqueColumn.trim().isEmpty();
    }
    
    // 过滤已存在的数据
    private DataBatch filterExistingData(DataBatch batch) {
        // 简单实现：如果有唯一列，检查数据是否已存在
        String uniqueColumn = config.getUniqueColumn();
        if (uniqueColumn == null || uniqueColumn.trim().isEmpty()) {
            return batch;
        }
        
        List<Map<String, Object>> filteredData = new ArrayList<>();
        
        try (Connection conn = dbManager.getSqlServerConnection()) {
            String checkSql = "SELECT COUNT(*) FROM " + config.getTargetTable() + " WHERE [" + uniqueColumn + "] = ?";
            
            try (PreparedStatement pstmt = conn.prepareStatement(checkSql)) {
                for (Map<String, Object> row : batch.getData()) {
                    Object uniqueValue = row.get(uniqueColumn);
                    if (uniqueValue != null) {
                        pstmt.setObject(1, uniqueValue);
                        try (ResultSet rs = pstmt.executeQuery()) {
                            if (rs.next() && rs.getInt(1) == 0) {
                                // 数据不存在，添加到过滤后的列表
                                filteredData.add(row);
                            }
                        }
                    } else {
                        // 唯一值为空，直接添加
                        filteredData.add(row);
                    }
                }
            }
        } catch (SQLException e) {
            System.err.println("过滤重复数据时出错: " + e.getMessage());
            return batch; // 出错时返回原批次
        }
        
        return new DataBatch(batch.getBatchNumber(), filteredData);
    }
    
    // 检查是否是重复键错误
    private boolean isDuplicateKeyError(Exception e) {
        String message = e.getMessage().toLowerCase();
        return message.contains("duplicate key") || 
               message.contains("primary key") ||
               message.contains("unique constraint") ||
               message.contains("violation of primary key") ||
               message.contains("violation of unique key");
    }
    
    // 带事务的批量插入
    private void insertBatchWithTransaction(List<Map<String, Object>> data, String threadName) throws SQLException {
        try (Connection conn = dbManager.getSqlServerConnection()) {
            conn.setAutoCommit(false);
            
            try (PreparedStatement pstmt = conn.prepareStatement(insertSql)) {
                for (Map<String, Object> row : data) {
                    for (int i = 0; i < columns.size(); i++) {
                        String columnName = columns.get(i).getName();
                        Object value = row.get(columnName);
                        pstmt.setObject(i + 1, value);
                    }
                    pstmt.addBatch();
                }
                
                pstmt.executeBatch();
                conn.commit();
                
            } catch (SQLException e) {
                conn.rollback();
                throw e;
            }
        }
    }
    
    // 保留原有的insertBatch方法作为备用
    private void insertBatch(List<Map<String, Object>> data, String threadName) throws SQLException {
        try (Connection conn = dbManager.getSqlServerConnection()) {
            conn.setAutoCommit(false);
            
            try (PreparedStatement pstmt = conn.prepareStatement(insertSql)) {
                for (Map<String, Object> row : data) {
                    for (int i = 0; i < columns.size(); i++) {
                        String columnName = columns.get(i).getName();
                        Object value = row.get(columnName);
                        pstmt.setObject(i + 1, value);
                    }
                    pstmt.addBatch();
                }
                
                pstmt.executeBatch();
                conn.commit();
                
            } catch (SQLException e) {
                conn.rollback();
                throw e;
            }
        }
    }
    
    public void stop() {
        running = false;
    }
}