package mysqlutil.mysqltock;

import java.sql.*;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/*
* feature_factor_info
*
feature_factor_record_aus
feature_factor_record_china
feature_factor_record_euro
feature_factor_record_japan
feature_factor_record_product
feature_factor_record_uk
feature_factor_record_us

foreign_exchange_record
*
* */
public class MySQLToClickHouseSync {
    private static final int BATCH_SIZE = 2000;  // 每个批次的大小
    private static final int THREAD_POOL_SIZE = 7;  // 线程池大小
    
    // 同步模式枚举
    public enum SyncMode {
        APPEND,     // 追加模式：直接追加数据到现有表
        RECREATE    // 重建模式：删除现有表并重新创建
    }

    public static void main(String[] args) {
        // ==================== 配置区域 ====================
        
        // 数据库连接配置
        String clickhouseUrl = "jdbc:clickhouse://192.168.5.111:8123";
        String mysqlUrl = "jdbc:mysql://192.168.3.138:3306/smartpath_admin?useSSL=false";
        String clickhouseUser = "default";
        String clickhousePassword = "smartpath";
        String mysqlUser = "root";
        //String mysqlPassword = "smartpath";
        String mysqlPassword = "smartpthdata";

        // 表配置
        String clickhouseTableName = "ods.online_processing_count";  // ClickHouse 表名 (database.table)
        String mysqlTableName = "online_processing_count";               // MySQL 表名
        String whereCondition = "1 = 1";                 // 可选的WHERE条件
        
        // ⭐ 同步模式配置 - 请在这里选择同步模式
        SyncMode syncMode = SyncMode.RECREATE;  // 改为 SyncMode.RECREATE 可删除重建表
        
        // ================================================

        try {
            // 解析ClickHouse表名
            String[] tableNameParts = clickhouseTableName.split("\\.");
            if (tableNameParts.length != 2) {
                throw new IllegalArgumentException("ClickHouse表名格式错误，应为 database.table 格式");
            }
            String clickhouseDatabase = tableNameParts[0];
            String clickhouseTable = tableNameParts[1];
            
            System.out.println("==================== MySQL到ClickHouse数据同步 ====================");
            System.out.println("源表: MySQL [" + mysqlTableName + "]");
            System.out.println("目标表: ClickHouse [" + clickhouseTableName + "]");
            System.out.println("同步模式: " + (syncMode == SyncMode.APPEND ? "追加模式" : "删除重建模式"));
            System.out.println("线程数: " + THREAD_POOL_SIZE + ", 批次大小: " + BATCH_SIZE);
            System.out.println("================================================================");
            
            // 处理ClickHouse表（检查存在性、删除重建或创建）
            handleClickHouseTable(mysqlUrl, mysqlUser, mysqlPassword, 
                                clickhouseUrl, clickhouseUser, clickhousePassword,
                                mysqlTableName, clickhouseDatabase, clickhouseTable, syncMode);
            
            // 获取MySQL表数据总数
            int totalCount = getMySQLTableCount(mysqlUrl, mysqlUser, mysqlPassword, mysqlTableName, whereCondition);
            System.out.println("MySQL表总记录数: " + totalCount);
            
            if (totalCount == 0) {
                System.out.println("没有数据需要同步");
                return;
            }
            
            // 创建线程池
            ExecutorService executor = Executors.newFixedThreadPool(THREAD_POOL_SIZE);
            
            // 计算每个线程处理的数据量
            int rowsPerThread = (totalCount + THREAD_POOL_SIZE - 1) / THREAD_POOL_SIZE;
            
            // 分发任务给多个线程
            for (int i = 0; i < THREAD_POOL_SIZE; i++) {
                int offset = i * rowsPerThread;
                int limit = Math.min(rowsPerThread, totalCount - offset);
                
                if (limit > 0) {
                    executor.submit(new MySQLToClickHouseTask(
                        mysqlUrl, mysqlUser, mysqlPassword,
                        clickhouseUrl, clickhouseUser, clickhousePassword,
                        mysqlTableName, clickhouseTableName,
                        whereCondition, offset, limit, i + 1
                    ));
                }
            }
            
            // 关闭线程池并等待完成
            executor.shutdown();
            executor.awaitTermination(2, TimeUnit.HOURS);
            
            System.out.println("✅ 数据同步完成: " + clickhouseTableName);
            
        } catch (Exception e) {
            System.err.println("❌ 同步失败: " + e.getMessage());
            e.printStackTrace();
        }
    }
    
    // 获取MySQL表记录总数
    private static int getMySQLTableCount(String mysqlUrl, String mysqlUser, String mysqlPassword, 
                                        String tableName, String whereCondition) throws SQLException {
        String countSQL = "SELECT COUNT(*) FROM " + tableName + 
                         (whereCondition.isEmpty() || "1=1".equals(whereCondition.trim()) ? "" : " WHERE " + whereCondition);
        
        try (Connection conn = DriverManager.getConnection(mysqlUrl, mysqlUser, mysqlPassword);
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(countSQL)) {
            
            if (rs.next()) {
                return rs.getInt(1);
            }
            return 0;
        }
    }
    
    // 处理ClickHouse表（统一的表处理逻辑）
    private static void handleClickHouseTable(String mysqlUrl, String mysqlUser, String mysqlPassword,
                                            String clickhouseUrl, String clickhouseUser, String clickhousePassword,
                                            String mysqlTableName, String clickhouseDatabase, String clickhouseTable, 
                                            SyncMode syncMode) throws SQLException {
        
        try (Connection clickhouseConn = DriverManager.getConnection(clickhouseUrl, clickhouseUser, clickhousePassword)) {
            
            // 检查表是否存在
            boolean tableExists = checkTableExists(clickhouseConn, clickhouseDatabase, clickhouseTable);
            
            if (tableExists) {
                System.out.println("ClickHouse表已存在: " + clickhouseDatabase + "." + clickhouseTable);
                
                if (syncMode == SyncMode.RECREATE) {
                    // 删除重建模式：删除现有表
                    System.out.println("删除重建模式：正在删除现有表...");
                    dropTable(clickhouseConn, clickhouseDatabase, clickhouseTable);
                    System.out.println("✅ 表删除成功，开始重新创建表...");
                    createClickHouseTable(mysqlUrl, mysqlUser, mysqlPassword, clickhouseConn, 
                                         mysqlTableName, clickhouseDatabase, clickhouseTable);
                } else {
                    // 追加模式：直接使用现有表
                    System.out.println("追加模式：将直接向现有表追加数据");
                }
            } else {
                // 表不存在，创建新表
                System.out.println("ClickHouse表不存在，开始根据MySQL表结构创建表...");
                createClickHouseTable(mysqlUrl, mysqlUser, mysqlPassword, clickhouseConn, 
                                     mysqlTableName, clickhouseDatabase, clickhouseTable);
            }
        }
    }
    
    // 检查表是否存在
    private static boolean checkTableExists(Connection clickhouseConn, String database, String table) throws SQLException {
        String checkTableSQL = "SELECT count(*) FROM system.tables WHERE database = ? AND name = ?";
        try (PreparedStatement checkStmt = clickhouseConn.prepareStatement(checkTableSQL)) {
            checkStmt.setString(1, database);
            checkStmt.setString(2, table);
            ResultSet rs = checkStmt.executeQuery();
            
            if (rs.next()) {
                return rs.getInt(1) > 0;
            }
            return false;
        }
    }
    
    // 删除表
    private static void dropTable(Connection clickhouseConn, String database, String table) throws SQLException {
        String dropSQL = "DROP TABLE IF EXISTS " + database + "." + table;
        try (Statement stmt = clickhouseConn.createStatement()) {
            stmt.executeUpdate(dropSQL);
        }
    }
    
    // 创建ClickHouse表
    private static void createClickHouseTable(String mysqlUrl, String mysqlUser, String mysqlPassword,
                                            Connection clickhouseConn, String mysqlTableName, 
                                            String clickhouseDatabase, String clickhouseTable) throws SQLException {
        
        // 获取MySQL表结构
        try (Connection mysqlConn = DriverManager.getConnection(mysqlUrl, mysqlUser, mysqlPassword)) {
            DatabaseMetaData metaData = mysqlConn.getMetaData();
            ResultSet columns = metaData.getColumns(null, null, mysqlTableName, null);
            
            StringBuilder createTableSQL = new StringBuilder();
            createTableSQL.append("CREATE TABLE ").append(clickhouseDatabase).append(".").append(clickhouseTable).append(" (");
            
            boolean hasColumns = false;
            Set<String> addedColumns = new HashSet<>();  // 用于防止重复列名
            
            while (columns.next()) {
                String columnName = columns.getString("COLUMN_NAME");
                
                // 防止重复列名（忽略大小写）
                String columnNameLower = columnName.toLowerCase();
                if (addedColumns.contains(columnNameLower)) {
                    System.out.println("⚠️ 跳过重复列: " + columnName);
                    continue;
                }
                addedColumns.add(columnNameLower);
                
                if (hasColumns) {
                    createTableSQL.append(", ");
                }
                
                String mysqlType = columns.getString("TYPE_NAME");
                int columnSize = columns.getInt("COLUMN_SIZE");
                int decimalDigits = columns.getInt("DECIMAL_DIGITS");
                
                String clickhouseType = mapMySQLTypeToClickHouse(mysqlType, columnSize, decimalDigits);
                createTableSQL.append("`").append(columnName).append("` ").append(clickhouseType);
                hasColumns = true;
                
                System.out.println("添加列: " + columnName + " (" + mysqlType + " -> " + clickhouseType + ")");
            }
            
            createTableSQL.append(") ENGINE = MergeTree() ORDER BY tuple()");
            
            if (hasColumns) {
                try (Statement createStmt = clickhouseConn.createStatement()) {
                    createStmt.executeUpdate(createTableSQL.toString());
                    System.out.println("✅ ClickHouse表创建成功: " + clickhouseDatabase + "." + clickhouseTable);
                }
            } else {
                throw new SQLException("无法获取MySQL表结构: " + mysqlTableName);
            }
        }
    }
    
    // MySQL数据类型映射到ClickHouse数据类型
    private static String mapMySQLTypeToClickHouse(String mysqlType, int columnSize, int decimalDigits) {
        mysqlType = mysqlType.toUpperCase();
        
        switch (mysqlType) {
            case "TINYINT":
                return "Int8";
            case "SMALLINT":
                return "Int16";
            case "MEDIUMINT":
            case "INT":
            case "INTEGER":
                return "Int32";
            case "BIGINT":
                return "Int64";
            case "FLOAT":
                return "Float32";
            case "DOUBLE":
            case "REAL":
                return "Float64";
            case "DECIMAL":
            case "NUMERIC":
                return "Decimal(" + columnSize + ", " + decimalDigits + ")";
            case "DATE":
                return "Date";
            case "TIME":
                return "String";
            case "DATETIME":
            case "TIMESTAMP":
                return "DateTime";
            case "CHAR":
            case "VARCHAR":
                return "String";
            case "TEXT":
            case "MEDIUMTEXT":
            case "LONGTEXT":
            case "TINYTEXT":
                return "String";
            case "BLOB":
            case "MEDIUMBLOB":
            case "LONGBLOB":
            case "TINYBLOB":
                return "String";
            case "JSON":
                return "String";
            case "BIT":
                return "UInt8";
            default:
                return "String";
        }
    }
    
    // 数据同步任务
    private static class MySQLToClickHouseTask implements Runnable {
        private final String mysqlUrl, mysqlUser, mysqlPassword;
        private final String clickhouseUrl, clickhouseUser, clickhousePassword;
        private final String mysqlTableName, clickhouseTableName;
        private final String whereCondition;
        private final int offset, limit, threadId;
        
        public MySQLToClickHouseTask(String mysqlUrl, String mysqlUser, String mysqlPassword,
                                   String clickhouseUrl, String clickhouseUser, String clickhousePassword,
                                   String mysqlTableName, String clickhouseTableName,
                                   String whereCondition, int offset, int limit, int threadId) {
            this.mysqlUrl = mysqlUrl;
            this.mysqlUser = mysqlUser;
            this.mysqlPassword = mysqlPassword;
            this.clickhouseUrl = clickhouseUrl;
            this.clickhouseUser = clickhouseUser;
            this.clickhousePassword = clickhousePassword;
            this.mysqlTableName = mysqlTableName;
            this.clickhouseTableName = clickhouseTableName;
            this.whereCondition = whereCondition;
            this.offset = offset;
            this.limit = limit;
            this.threadId = threadId;
        }
        
        @Override
        public void run() {
            long startTime = System.currentTimeMillis();
            String threadName = "Thread-" + threadId;
            
            System.out.println("[" + threadName + "] 开始处理数据，偏移量: " + offset + "，限制: " + limit);
            
            try (Connection mysqlConn = DriverManager.getConnection(mysqlUrl, mysqlUser, mysqlPassword);
                 Connection clickhouseConn = DriverManager.getConnection(clickhouseUrl, clickhouseUser, clickhousePassword)) {
                
                // 构建查询SQL
                String selectSQL = "SELECT * FROM " + mysqlTableName;
                if (!whereCondition.isEmpty() && !"1=1".equals(whereCondition.trim())) {
                    selectSQL += " WHERE " + whereCondition;
                }
                selectSQL += " LIMIT " + limit + " OFFSET " + offset;
                
                // 查询MySQL数据
                try (Statement selectStmt = mysqlConn.createStatement();
                     ResultSet rs = selectStmt.executeQuery(selectSQL)) {
                    
                    ResultSetMetaData metaData = rs.getMetaData();
                    int columnCount = metaData.getColumnCount();
                    
                    // 构建ClickHouse插入SQL
                    StringBuilder insertSQL = new StringBuilder("INSERT INTO " + clickhouseTableName + " (");
                    for (int i = 1; i <= columnCount; i++) {
                        if (i > 1) insertSQL.append(", ");
                        insertSQL.append("`").append(metaData.getColumnName(i)).append("`");
                    }
                    insertSQL.append(") VALUES (");
                    for (int i = 1; i <= columnCount; i++) {
                        if (i > 1) insertSQL.append(", ");
                        insertSQL.append("?");
                    }
                    insertSQL.append(")");
                    
                    try (PreparedStatement insertStmt = clickhouseConn.prepareStatement(insertSQL.toString())) {
                        
                        int batchCount = 0;
                        int totalInserted = 0;
                        int batchNumber = 1;
                        
                        while (rs.next()) {
                            // 设置参数
                            for (int i = 1; i <= columnCount; i++) {
                                Object value = rs.getObject(i);
                                insertStmt.setObject(i, value);
                            }
                            insertStmt.addBatch();
                            batchCount++;
                            
                            // 批量执行
                            if (batchCount >= BATCH_SIZE) {
                                long batchStartTime = System.currentTimeMillis();
                                insertStmt.executeBatch();
                                long batchEndTime = System.currentTimeMillis();
                                
                                totalInserted += batchCount;
                                System.out.println(String.format(
                                    "[%s] 批次 %d 完成: 插入 %d 条数据，耗时 %d ms，总进度: %d",
                                    threadName, batchNumber, batchCount, 
                                    (batchEndTime - batchStartTime), totalInserted
                                ));
                                
                                batchCount = 0;
                                batchNumber++;
                                insertStmt.clearBatch();
                            }
                        }
                        
                        // 处理剩余数据
                        if (batchCount > 0) {
                            long batchStartTime = System.currentTimeMillis();
                            insertStmt.executeBatch();
                            long batchEndTime = System.currentTimeMillis();
                            
                            totalInserted += batchCount;
                            System.out.println(String.format(
                                "[%s] 最后批次 %d 完成: 插入 %d 条数据，耗时 %d ms",
                                threadName, batchNumber, batchCount, (batchEndTime - batchStartTime)
                            ));
                        }
                        
                        long endTime = System.currentTimeMillis();
                        long totalTime = endTime - startTime;
                        double avgSpeed = totalInserted / (totalTime / 1000.0);
                        
                        System.out.println(String.format(
                            "[%s] ✅ 线程完成! 总计插入: %d 条数据，总耗时: %d ms，平均速度: %.2f 条/秒",
                            threadName, totalInserted, totalTime, avgSpeed
                        ));
                    }
                }
                
            } catch (SQLException e) {
                System.err.println("[" + threadName + "] ❌ 同步失败: " + e.getMessage());
                e.printStackTrace();
            }
        }
    }
}