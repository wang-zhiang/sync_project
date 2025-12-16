package sqlservertomysql.service;

import sqlservertomysql.config.DatabaseConfig;
import sqlservertomysql.model.ColumnInfo;

import java.sql.*;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 数据同步服务类
 */
public class DataSyncService {
    private final DatabaseConfig config;
    private final TableStructureService tableStructureService;
    private final AtomicLong totalSyncedRecords = new AtomicLong(0);
    
    public DataSyncService(DatabaseConfig config) {
        this.config = config;
        this.tableStructureService = new TableStructureService();
    }

    /**
     * 同步指定表的数据（支持不同的源表名和目标表名）
     */
    public void syncTable(String sqlServerTableName, String mysqlTableName) throws Exception {
        System.out.println("开始同步表: " + sqlServerTableName + " -> " + mysqlTableName);
        
        // 获取数据库连接
        try (Connection sqlServerConn = getSqlServerConnection();
             Connection mysqlConn = getMySQLConnection()) {
            
            // 获取SQL Server表结构
            List<ColumnInfo> columns = tableStructureService.getSqlServerTableStructure(sqlServerConn, sqlServerTableName);
            if (columns.isEmpty()) {
                throw new RuntimeException("表 " + sqlServerTableName + " 不存在或没有列信息");
            }
            
            // 检查并创建MySQL表
            if (!tableStructureService.tableExists(mysqlConn, mysqlTableName)) {
                System.out.println("MySQL中不存在表 " + mysqlTableName + "，正在创建...");
                tableStructureService.createMySQLTable(mysqlConn, mysqlTableName, columns);
                System.out.println("表 " + mysqlTableName + " 创建成功");
            }
            
            // 获取总记录数
            long totalRecords = getTotalRecords(sqlServerConn, sqlServerTableName);
            System.out.println("表 " + sqlServerTableName + " 总记录数: " + totalRecords);
            
            if (totalRecords == 0) {
                System.out.println("表 " + sqlServerTableName + " 没有数据需要同步");
                return;
            }
            
            // 计算分页数
            int pageSize = config.getPageSize();
            long totalPages = (totalRecords + pageSize - 1) / pageSize;
            
            // 创建线程池
            ExecutorService executor = Executors.newFixedThreadPool(config.getThreadCount());
            
            // 分页同步数据
            for (long page = 0; page < totalPages; page++) {
                final long currentPage = page;
                final long offset = page * pageSize;
                
                executor.submit(() -> {
                    try {
                        syncDataPage(sqlServerTableName, mysqlTableName, columns, offset, pageSize, currentPage + 1, totalPages);
                    } catch (Exception e) {
                        System.err.println("同步第 " + (currentPage + 1) + " 页数据时出错: " + e.getMessage());
                        e.printStackTrace();
                    }
                });
            }
            
            // 等待所有任务完成
            executor.shutdown();
            if (!executor.awaitTermination(30, TimeUnit.MINUTES)) {
                System.err.println("同步任务超时，强制关闭线程池");
                executor.shutdownNow();
            }
            
            System.out.println("表 " + sqlServerTableName + " -> " + mysqlTableName + " 同步完成，总共同步了 " + totalSyncedRecords.get() + " 条记录");
        }
    }
    
    /**
     * 同步单页数据
     */
    private void syncDataPage(String sqlServerTableName, String mysqlTableName, List<ColumnInfo> columns, 
                             long offset, int pageSize, long currentPage, long totalPages) throws Exception {
        
        try (Connection sqlServerConn = getSqlServerConnection();
             Connection mysqlConn = getMySQLConnection()) {
            
            // 构建查询SQL（兼容SQL Server 2008的分页语法）
            StringBuilder selectSql = new StringBuilder();
            selectSql.append("SELECT ");
            for (int i = 0; i < columns.size(); i++) {
                selectSql.append("[").append(columns.get(i).getColumnName()).append("]");
                if (i < columns.size() - 1) {
                    selectSql.append(", ");
                }
            }
            selectSql.append(" FROM (");
            selectSql.append("SELECT ");
            for (int i = 0; i < columns.size(); i++) {
                selectSql.append("[").append(columns.get(i).getColumnName()).append("]");
                if (i < columns.size() - 1) {
                    selectSql.append(", ");
                }
            }
            selectSql.append(", ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) as rn");
            selectSql.append(" FROM [").append(sqlServerTableName).append("] ");
            selectSql.append("WHERE ").append(config.getSyncCondition());
            selectSql.append(") t WHERE rn > ? AND rn <= ?");
            
            // 构建插入SQL
            StringBuilder insertSql = new StringBuilder();
            insertSql.append("INSERT INTO `").append(mysqlTableName).append("` (");
            for (int i = 0; i < columns.size(); i++) {
                insertSql.append("`").append(columns.get(i).getColumnName()).append("`");
                if (i < columns.size() - 1) {
                    insertSql.append(", ");
                }
            }
            insertSql.append(") VALUES (");
            for (int i = 0; i < columns.size(); i++) {
                insertSql.append("?");
                if (i < columns.size() - 1) {
                    insertSql.append(", ");
                }
            }
            insertSql.append(") ON DUPLICATE KEY UPDATE ");
            
            // 构建更新子句
            boolean hasNonPrimaryKey = false;
            for (ColumnInfo column : columns) {
                if (!column.isPrimaryKey()) {
                    if (hasNonPrimaryKey) {
                        insertSql.append(", ");
                    }
                    insertSql.append("`").append(column.getColumnName()).append("`=VALUES(`")
                             .append(column.getColumnName()).append("`)");
                    hasNonPrimaryKey = true;
                }
            }
            
            if (!hasNonPrimaryKey) {
                // 如果没有非主键列，使用主键列进行更新
                for (int i = 0; i < columns.size(); i++) {
                    if (i > 0) insertSql.append(", ");
                    String colName = columns.get(i).getColumnName();
                    insertSql.append("`").append(colName).append("`=VALUES(`").append(colName).append("`)");
                }
            }
            
            try (PreparedStatement selectStmt = sqlServerConn.prepareStatement(selectSql.toString());
                 PreparedStatement insertStmt = mysqlConn.prepareStatement(insertSql.toString())) {
                
                selectStmt.setLong(1, offset);
                selectStmt.setLong(2, offset + pageSize);
                
                try (ResultSet rs = selectStmt.executeQuery()) {
                    int batchCount = 0;
                    int recordCount = 0;
                    
                    while (rs.next()) {
                        for (int i = 1; i <= columns.size(); i++) {
                            insertStmt.setObject(i, rs.getObject(i));
                        }
                        insertStmt.addBatch();
                        batchCount++;
                        recordCount++;
                        
                        // 使用配置的批次大小
                        if (batchCount >= config.getBatchSize()) {
                            insertStmt.executeBatch();
                            insertStmt.clearBatch();
                            batchCount = 0;
                        }
                    }
                    
                    // 执行剩余的批处理
                    if (batchCount > 0) {
                        insertStmt.executeBatch();
                    }
                    
                    totalSyncedRecords.addAndGet(recordCount);
                    
                    // 计算进度百分比
                    double progress = (double) currentPage / totalPages * 100;
                    long totalSynced = totalSyncedRecords.get();
                    
                    // 简化的进度日志 - 一行显示所有关键信息
                    System.out.println(String.format("进度: %d/%d页 (%.1f%%) | 已同步: %,d条记录 | 本页: %d条", 
                                                    currentPage, totalPages, progress, totalSynced, recordCount));
                }
            }
        }
    }
    
    /**
     * 获取表的总记录数
     */
    private long getTotalRecords(Connection conn, String tableName) throws SQLException {
        String sql = "SELECT COUNT(*) FROM [" + tableName + "] WHERE " + config.getSyncCondition();
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            if (rs.next()) {
                return rs.getLong(1);
            }
        }
        return 0;
    }
    
    /**
     * 获取SQL Server连接
     */
    private Connection getSqlServerConnection() throws SQLException {
        return DriverManager.getConnection(
            config.getSqlServerUrl(),
            config.getSqlServerUsername(),
            config.getSqlServerPassword()
        );
    }
    
    /**
     * 获取MySQL连接
     */
    private Connection getMySQLConnection() throws SQLException {
        return DriverManager.getConnection(
            config.getMysqlUrl(),
            config.getMysqlUsername(),
            config.getMysqlPassword()
        );
    }
}