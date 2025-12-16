import java.sql.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * ClickHouse到MySQL数据同步工具
 * 支持特定表的全量同步，自动建表，多线程批量处理
 */
public class ClickHouseToMySQLSyncer {
    
    // 配置常量
    private static final int BATCH_SIZE = 10000;
    private static final int THREAD_POOL_SIZE = 4;
    
    // 数据库连接配置
    private final String clickHouseUrl;
    private final String clickHouseUser;
    private final String clickHousePassword;
    
    private final String mysqlUrl;
    private final String mysqlUser;
    private final String mysqlPassword;
    
    // 线程池
    private final ExecutorService executorService;
    
    // 统计信息
    private final AtomicLong totalProcessed = new AtomicLong(0);
    
    public ClickHouseToMySQLSyncer(String clickHouseUrl, String clickHouseUser, String clickHousePassword,
                                   String mysqlUrl, String mysqlUser, String mysqlPassword) {
        this.clickHouseUrl = clickHouseUrl;
        this.clickHouseUser = clickHouseUser;
        this.clickHousePassword = clickHousePassword;
        this.mysqlUrl = mysqlUrl;
        this.mysqlUser = mysqlUser;
        this.mysqlPassword = mysqlPassword;
        
        this.executorService = Executors.newFixedThreadPool(THREAD_POOL_SIZE);
    }
    
    /**
     * 同步指定表的数据
     */
    public void syncTable(String tableName) throws Exception {
        System.out.println("开始同步表: " + tableName);
        
        try (Connection chConn = getClickHouseConnection();
             Connection mysqlConn = getMySQLConnection()) {
            
            // 1. 检查MySQL表是否存在
            if (checkTableExists(mysqlConn, tableName)) {
                throw new RuntimeException("错误：MySQL中表 '" + tableName + "' 已存在！请先删除或重命名该表。");
            }
            
            // 2. 获取ClickHouse表结构
            List<ColumnInfo> columns = getTableStructure(chConn, tableName);
            if (columns.isEmpty()) {
                throw new RuntimeException("错误：ClickHouse中表 '" + tableName + "' 不存在或无列信息！");
            }
            
            // 3. 在MySQL中创建表
            createMySQLTable(mysqlConn, tableName, columns);
            System.out.println("MySQL表创建成功: " + tableName);
            
            // 4. 获取数据总数
            long totalCount = getTableRowCount(chConn, tableName);
            System.out.println("总数据量: " + totalCount + " 条");
            
            // 5. 多线程批量同步数据
            syncDataInBatches(tableName, columns, totalCount);
            
            System.out.println("同步完成！总共处理: " + totalProcessed.get() + " 条数据");
            
        } finally {
            executorService.shutdown();
            try {
                if (!executorService.awaitTermination(60, TimeUnit.SECONDS)) {
                    executorService.shutdownNow();
                }
            } catch (InterruptedException e) {
                executorService.shutdownNow();
            }
        }
    }   
 /**
     * 获取ClickHouse连接
     */
    private Connection getClickHouseConnection() throws SQLException {
        return DriverManager.getConnection(clickHouseUrl, clickHouseUser, clickHousePassword);
    }
    
    /**
     * 获取MySQL连接
     */
    private Connection getMySQLConnection() throws SQLException {
        return DriverManager.getConnection(mysqlUrl, mysqlUser, mysqlPassword);
    }
    
    /**
     * 检查MySQL表是否存在
     */
    private boolean checkTableExists(Connection conn, String tableName) throws SQLException {
        DatabaseMetaData metaData = conn.getMetaData();
        try (ResultSet rs = metaData.getTables(null, null, tableName, new String[]{"TABLE"})) {
            return rs.next();
        }
    }
    
    /**
     * 获取ClickHouse表结构
     */
    private List<ColumnInfo> getTableStructure(Connection conn, String tableName) throws SQLException {
        List<ColumnInfo> columns = new ArrayList<>();
        
        String sql = "DESCRIBE TABLE " + tableName;
        try (PreparedStatement stmt = conn.prepareStatement(sql);
             ResultSet rs = stmt.executeQuery()) {
            
            while (rs.next()) {
                String columnName = rs.getString("name");
                String clickHouseType = rs.getString("type");
                String mysqlType = convertClickHouseTypeToMySQL(clickHouseType);
                
                columns.add(new ColumnInfo(columnName, clickHouseType, mysqlType));
            }
        }
        
        return columns;
    }
    
    /**
     * ClickHouse数据类型转MySQL数据类型
     */
    private String convertClickHouseTypeToMySQL(String clickHouseType) {
        clickHouseType = clickHouseType.toLowerCase();
        
        // 数值类型
        if (clickHouseType.startsWith("int8")) return "TINYINT";
        if (clickHouseType.startsWith("int16")) return "SMALLINT";
        if (clickHouseType.startsWith("int32")) return "INT";
        if (clickHouseType.startsWith("int64")) return "BIGINT";
        if (clickHouseType.startsWith("uint8")) return "TINYINT UNSIGNED";
        if (clickHouseType.startsWith("uint16")) return "SMALLINT UNSIGNED";
        if (clickHouseType.startsWith("uint32")) return "INT UNSIGNED";
        if (clickHouseType.startsWith("uint64")) return "BIGINT UNSIGNED";
        if (clickHouseType.startsWith("float32")) return "FLOAT";
        if (clickHouseType.startsWith("float64")) return "DOUBLE";
        
        // 字符串类型
        if (clickHouseType.startsWith("string")) return "TEXT";
        if (clickHouseType.startsWith("fixedstring")) {
            // 提取长度
            int start = clickHouseType.indexOf('(');
            int end = clickHouseType.indexOf(')');
            if (start > 0 && end > start) {
                String length = clickHouseType.substring(start + 1, end);
                return "VARCHAR(" + length + ")";
            }
            return "VARCHAR(255)";
        }
        
        // 日期时间类型
        if (clickHouseType.startsWith("date")) return "DATE";
        if (clickHouseType.startsWith("datetime")) return "DATETIME";
        if (clickHouseType.startsWith("timestamp")) return "TIMESTAMP";
        
        // 默认类型
        return "TEXT";
    }    /**

     * 在MySQL中创建表
     */
    private void createMySQLTable(Connection conn, String tableName, List<ColumnInfo> columns) throws SQLException {
        StringBuilder sql = new StringBuilder();
        sql.append("CREATE TABLE ").append(tableName).append(" (");
        
        for (int i = 0; i < columns.size(); i++) {
            ColumnInfo column = columns.get(i);
            sql.append(column.name).append(" ").append(column.mysqlType);
            
            if (i < columns.size() - 1) {
                sql.append(", ");
            }
        }
        
        sql.append(") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4");
        
        try (PreparedStatement stmt = conn.prepareStatement(sql.toString())) {
            stmt.executeUpdate();
        }
    }
    
    /**
     * 获取表行数
     */
    private long getTableRowCount(Connection conn, String tableName) throws SQLException {
        String sql = "SELECT COUNT(*) FROM " + tableName;
        try (PreparedStatement stmt = conn.prepareStatement(sql);
             ResultSet rs = stmt.executeQuery()) {
            
            if (rs.next()) {
                return rs.getLong(1);
            }
            return 0;
        }
    }
    
    /**
     * 多线程批量同步数据
     */
    private void syncDataInBatches(String tableName, List<ColumnInfo> columns, long totalCount) throws Exception {
        long batchCount = (totalCount + BATCH_SIZE - 1) / BATCH_SIZE;
        List<Future<Void>> futures = new ArrayList<>();
        
        System.out.println("开始多线程同步，总批次: " + batchCount);
        
        for (long i = 0; i < batchCount; i++) {
            final long offset = i * BATCH_SIZE;
            final long currentBatch = i + 1;
            
            Future<Void> future = executorService.submit(() -> {
                try {
                    syncBatch(tableName, columns, offset, BATCH_SIZE, currentBatch, batchCount);
                    return null;
                } catch (Exception e) {
                    System.err.println("批次 " + currentBatch + " 处理失败: " + e.getMessage());
                    throw new RuntimeException(e);
                }
            });
            
            futures.add(future);
        }
        
        // 等待所有任务完成
        for (Future<Void> future : futures) {
            future.get();
        }
    }
    
    /**
     * 同步单个批次的数据
     */
    private void syncBatch(String tableName, List<ColumnInfo> columns, long offset, int limit, 
                          long currentBatch, long totalBatches) throws SQLException {
        
        try (Connection chConn = getClickHouseConnection();
             Connection mysqlConn = getMySQLConnection()) {
            
            // 从ClickHouse读取数据
            String selectSql = "SELECT * FROM " + tableName + " LIMIT " + limit + " OFFSET " + offset;
            
            // 准备MySQL插入语句
            StringBuilder insertSql = new StringBuilder();
            insertSql.append("INSERT INTO ").append(tableName).append(" (");
            
            for (int i = 0; i < columns.size(); i++) {
                insertSql.append(columns.get(i).name);
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
            insertSql.append(")");
            
            try (PreparedStatement selectStmt = chConn.prepareStatement(selectSql);
                 PreparedStatement insertStmt = mysqlConn.prepareStatement(insertSql.toString())) {
                
                mysqlConn.setAutoCommit(false);
                
                try (ResultSet rs = selectStmt.executeQuery()) {
                    int batchCount = 0;
                    
                    while (rs.next()) {
                        for (int i = 1; i <= columns.size(); i++) {
                            insertStmt.setObject(i, rs.getObject(i));
                        }
                        insertStmt.addBatch();
                        batchCount++;
                        
                        if (batchCount % 1000 == 0) {
                            insertStmt.executeBatch();
                            insertStmt.clearBatch();
                        }
                    }
                    
                    // 执行剩余的批次
                    if (batchCount % 1000 != 0) {
                        insertStmt.executeBatch();
                    }
                    
                    mysqlConn.commit();
                    
                    long processed = totalProcessed.addAndGet(batchCount);
                    System.out.println(String.format("线程 %s - 批次 %d/%d 完成，本批次: %d 条，总计: %d 条", 
                        Thread.currentThread().getName(), currentBatch, totalBatches, batchCount, processed));
                    
                } catch (SQLException e) {
                    mysqlConn.rollback();
                    throw e;
                } finally {
                    mysqlConn.setAutoCommit(true);
                }
            }
        }
    }
    private static class ColumnInfo {
        final String name;
        final String clickHouseType;
        final String mysqlType;
        
        ColumnInfo(String name, String clickHouseType, String mysqlType) {
            this.name = name;
            this.clickHouseType = clickHouseType;
            this.mysqlType = mysqlType;
        }
    }
    
    /**
     * 使用示例
     */
    public static void main(String[] args) {
        // 配置数据库连接信息
        String clickHouseUrl = "jdbc:clickhouse://localhost:8123/default";
        String clickHouseUser = "default";
        String clickHousePassword = "";
        
        String mysqlUrl = "jdbc:mysql://localhost:3306/test_db?useSSL=false&serverTimezone=UTC";
        String mysqlUser = "root";
        String mysqlPassword = "password";
        
        // 创建同步器
        ClickHouseToMySQLSyncer syncer = new ClickHouseToMySQLSyncer(
            clickHouseUrl, clickHouseUser, clickHousePassword,
            mysqlUrl, mysqlUser, mysqlPassword
        );
        
        try {
            // 同步指定表
            String tableName = "your_table_name";
            syncer.syncTable(tableName);
            
        } catch (Exception e) {
            System.err.println("同步失败: " + e.getMessage());
            e.printStackTrace();
        }
    }
}