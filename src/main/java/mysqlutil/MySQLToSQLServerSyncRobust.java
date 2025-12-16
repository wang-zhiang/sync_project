package mysqlutil;

import java.sql.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * MySQL到SQLServer高性能数据同步工具 - 增强版
 * 特点：
 * 1. 多线程同步（3个线程）
 * 2. 批量处理（每批3000条记录）
 * 3. 动态分页处理（不依赖特定字段）
 * 4. 增强的错误处理和重试机制
 * 5. 事务支持，确保数据一致性
 */
public class MySQLToSQLServerSyncRobust {

    // 源数据库配置 (MySQL)
    private static String SOURCE_HOST = "101.89.122.158";
    private static String SOURCE_PORT = "3306";
    private static String SOURCE_DATABASE = "eleme_app";
    private static String SOURCE_TABLE = "pijiu_shop_eleme_new_address";
    private static String SOURCE_URL = "";
    private static String SOURCE_USERNAME = "root";
    private static String SOURCE_PASSWORD = "smartpath@123";

    // 目标数据库配置 (SQL Server)
    private static String TARGET_HOST = "192.168.4.39";
    private static String TARGET_PORT = "1433";
    private static String TARGET_DATABASE = "o2o";
    private static String TARGET_TABLE = "pijiu_shop_eleme_new_address_20250718_new";
    private static String TARGET_URL = "";
    private static String TARGET_USERNAME = "sa";
    private static String TARGET_PASSWORD = "smartpthdata";

    // 同步配置
    private static final int THREAD_COUNT = 5; // 使用3个线程
    private static final int BATCH_SIZE = 5000; // 每批3000条记录
    private static final int MAX_RETRY_COUNT = 5; // 最大重试次数
    private static final int CONNECTION_RETRY_DELAY = 5000; // 连接重试延迟（毫秒）
    private static final int CONNECTION_TIMEOUT = 60000; // 连接超时（毫秒）
    private static final int SOCKET_TIMEOUT = 300000; // Socket超时（毫秒）
    private static final int QUERY_TIMEOUT = 600; // 查询超时（秒）

    // 进度监控
    private static AtomicLong processedRecords = new AtomicLong(0);
    private static AtomicLong totalRecords = new AtomicLong(0);
    private static AtomicLong failedRecords = new AtomicLong(0);
    private static long startTime;

    // 数据类型映射
    private static final Map<String, String> TYPE_MAPPING = new HashMap<String, String>() {{
        put("TINYINT", "TINYINT");
        put("SMALLINT", "SMALLINT");
        put("MEDIUMINT", "INT");
        put("INT", "INT");
        put("INTEGER", "INT");
        put("BIGINT", "BIGINT");
        put("FLOAT", "FLOAT");
        put("DOUBLE", "FLOAT");
        put("DECIMAL", "DECIMAL");
        put("NUMERIC", "DECIMAL");
        put("DATE", "DATE");
        put("TIME", "TIME");
        put("DATETIME", "DATETIME");
        put("TIMESTAMP", "DATETIME");
        put("YEAR", "SMALLINT");
        put("CHAR", "CHAR");
        put("VARCHAR", "NVARCHAR");
        put("BINARY", "BINARY");
        put("VARBINARY", "VARBINARY");
        put("TINYBLOB", "VARBINARY(255)");
        put("BLOB", "VARBINARY(MAX)");
        put("MEDIUMBLOB", "VARBINARY(MAX)");
        put("LONGBLOB", "VARBINARY(MAX)");
        put("TINYTEXT", "NVARCHAR(255)");
        put("TEXT", "NVARCHAR(MAX)");
        put("MEDIUMTEXT", "NVARCHAR(MAX)");
        put("LONGTEXT", "NVARCHAR(MAX)");
        put("JSON", "NVARCHAR(MAX)");
        put("BIT", "BIT");
    }};

    public static void main(String[] args) {
        // 使用已经配置好的连接信息，无需交互式输入
        // 如果需要通过命令行参数覆盖默认配置，可以使用以下代码
        if (args.length >= 7) {
            SOURCE_HOST = args[0];
            SOURCE_DATABASE = args[1];
            SOURCE_TABLE = args[2];
            SOURCE_USERNAME = args[3];
            SOURCE_PASSWORD = args[4];
            TARGET_HOST = args[5];
            TARGET_DATABASE = args[6];
            TARGET_TABLE = args.length > 7 ? args[7] : SOURCE_TABLE;
            TARGET_USERNAME = args.length > 8 ? args[8] : "sa";
            TARGET_PASSWORD = args.length > 9 ? args[9] : TARGET_PASSWORD;
        }

        // 构建连接URL
        SOURCE_URL = "jdbc:mysql://" + SOURCE_HOST + ":" + SOURCE_PORT + "/" + SOURCE_DATABASE
                + "?useSSL=false&serverTimezone=UTC&allowPublicKeyRetrieval=true&rewriteBatchedStatements=true"
                + "&connectTimeout=" + CONNECTION_TIMEOUT + "&socketTimeout=" + SOCKET_TIMEOUT
                + "&autoReconnect=true&failOverReadOnly=false&maxReconnects=5&initialTimeout=2"
                + "&useUnicode=true&characterEncoding=utf8";

        TARGET_URL = "jdbc:sqlserver://" + TARGET_HOST + ":" + TARGET_PORT + ";databaseName=" + TARGET_DATABASE
                + ";trustServerCertificate=true;connectTimeout=" + CONNECTION_TIMEOUT
                + ";socketTimeout=" + SOCKET_TIMEOUT + ";loginTimeout=" + (CONNECTION_TIMEOUT / 1000);

        System.out.println("=== MySQL到SQLServer数据同步工具 (增强版) ===\n");
        System.out.println("源数据库: " + SOURCE_HOST + ":" + SOURCE_PORT + "/" + SOURCE_DATABASE + "." + SOURCE_TABLE);
        System.out.println("目标数据库: " + TARGET_HOST + ":" + TARGET_PORT + "/" + TARGET_DATABASE + "." + TARGET_TABLE);
        System.out.println("线程数量: " + THREAD_COUNT);
        System.out.println("批处理大小: " + BATCH_SIZE);

        startTime = System.currentTimeMillis();

        try {
            // 加载数据库驱动
            loadDatabaseDrivers();
            
            // 测试数据库连接
            if (!testConnections()) {
                System.err.println("❌ 数据库连接测试失败，程序退出");
                return;
            }

            // 检查并创建目标表
            if (!ensureTargetTableExists()) {
                System.err.println("❌ 目标表创建失败，程序退出");
                return;
            }

            // 获取源表总记录数
            long total = getTotalRecords();
            if (total <= 0) {
                System.out.println("❌ 源表没有数据或获取记录数失败");
                return;
            }

            totalRecords.set(total);
            System.out.println("📊 源表总记录数: " + total);

            // 启动进度监控线程
            startProgressMonitor();
            
            // 执行同步
            performSync(total);

        } catch (Exception e) {
            System.err.println("❌ 同步过程中发生错误: " + e.getMessage());
            e.printStackTrace();
        }
    }

    /**
     * 加载数据库驱动
     */
    private static void loadDatabaseDrivers() {
        try {
            Class.forName("com.mysql.cj.jdbc.Driver");
            Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
            System.out.println("✅ 数据库驱动加载成功");
        } catch (ClassNotFoundException e) {
            System.err.println("❌ 数据库驱动加载失败: " + e.getMessage());
            System.err.println("请确保已添加MySQL和SQL Server JDBC驱动到类路径");
            System.exit(1);
        }
    }

    /**
     * 创建数据库连接（带重试机制）
     */
    private static Connection createConnectionWithRetry(String url, String username, String password, String dbType) {
        for (int retry = 0; retry < MAX_RETRY_COUNT; retry++) {
            try {
                Connection conn = DriverManager.getConnection(url, username, password);

                if ("mysql".equals(dbType)) {
                    conn.setAutoCommit(true);
                    try (Statement stmt = conn.createStatement()) {
                        stmt.execute("SET SESSION wait_timeout = 7200");
                        stmt.execute("SET SESSION interactive_timeout = 7200");
                        stmt.execute("SET SESSION net_read_timeout = 600");
                        stmt.execute("SET SESSION net_write_timeout = 600");
                    }
                } else if ("sqlserver".equals(dbType)) {
                    conn.setAutoCommit(false); // 使用事务
                }

                System.out.println("✅ " + dbType + "连接成功 (尝试" + (retry + 1) + "次)");
                return conn;

            } catch (SQLException e) {
                System.err.println("❌ " + dbType + "连接失败 (尝试" + (retry + 1) + "次): " + e.getMessage());
                if (retry < MAX_RETRY_COUNT - 1) {
                    try {
                        // 指数退避策略
                        long delay = CONNECTION_RETRY_DELAY * (long)Math.pow(2, retry);
                        System.out.println("⏱️ 等待" + (delay / 1000) + "秒后重试...");
                        Thread.sleep(delay);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        break;
                    }
                }
            }
        }
        return null;
    }

    /**
     * 验证并在必要时重新创建连接
     */
    private static Connection validateAndRecreateConnection(Connection conn, String url, String username, String password, String dbType) {
        try {
            if (conn == null || conn.isClosed() || !conn.isValid(10)) {
                if (conn != null) {
                    try { conn.close(); } catch (SQLException ignored) {}
                }
                System.out.println("🔄 重新创建" + dbType + "连接...");
                return createConnectionWithRetry(url, username, password, dbType);
            }
            return conn;
        } catch (SQLException e) {
            System.err.println("⚠️ 连接验证失败: " + e.getMessage());
            try { if (conn != null) conn.close(); } catch (SQLException ignored) {}
            return createConnectionWithRetry(url, username, password, dbType);
        }
    }

    /**
     * 测试数据库连接
     */
    private static boolean testConnections() {
        System.out.println("\n🔗 测试数据库连接...");

        Connection mysqlConn = createConnectionWithRetry(SOURCE_URL, SOURCE_USERNAME, SOURCE_PASSWORD, "mysql");
        if (mysqlConn == null) return false;
        try { mysqlConn.close(); } catch (SQLException ignored) {}

        Connection sqlServerConn = createConnectionWithRetry(TARGET_URL, TARGET_USERNAME, TARGET_PASSWORD, "sqlserver");
        if (sqlServerConn == null) return false;
        try { sqlServerConn.close(); } catch (SQLException ignored) {}

        return true;
    }

    /**
     * 确保目标表存在，如果不存在则创建
     */
    private static boolean ensureTargetTableExists() {
        System.out.println("\n🏗️ 检查目标表...");

        Connection sourceConn = null;
        Connection targetConn = null;

        try {
            sourceConn = createConnectionWithRetry(SOURCE_URL, SOURCE_USERNAME, SOURCE_PASSWORD, "mysql");
            targetConn = createConnectionWithRetry(TARGET_URL, TARGET_USERNAME, TARGET_PASSWORD, "sqlserver");

            if (sourceConn == null || targetConn == null) {
                return false;
            }

            // 检查目标表是否存在
            if (tableExists(targetConn, TARGET_TABLE)) {
                System.out.println("✅ 目标表已存在: " + TARGET_TABLE);
                return true;
            }

            // 获取源表结构
            System.out.println("📋 获取源表结构...");
            String createTableSQL = generateCreateTableSQL(sourceConn);

            // 创建目标表
            System.out.println("🏗️ 创建目标表...");
            try (Statement stmt = targetConn.createStatement()) {
                stmt.execute(createTableSQL);
                targetConn.commit();
                System.out.println("✅ 目标表创建成功");
                System.out.println("📝 建表语句: " + createTableSQL);
                return true;
            }

        } catch (SQLException e) {
            System.err.println("❌ 检查/创建目标表失败: " + e.getMessage());
            e.printStackTrace();
            return false;
        } finally {
            if (sourceConn != null) try { sourceConn.close(); } catch (SQLException ignored) {}
            if (targetConn != null) try { targetConn.close(); } catch (SQLException ignored) {}
        }
    }

    /**
     * 检查表是否存在
     */
    private static boolean tableExists(Connection conn, String tableName) throws SQLException {
        DatabaseMetaData metaData = conn.getMetaData();
        try (ResultSet rs = metaData.getTables(null, null, tableName, new String[]{"TABLE"})) {
            return rs.next();
        }
    }

    /**
     * 生成创建表的SQL语句
     */
    private static String generateCreateTableSQL(Connection sourceConn) throws SQLException {
        StringBuilder sql = new StringBuilder();
        sql.append("CREATE TABLE ").append(TARGET_TABLE).append(" (\n");

        DatabaseMetaData metaData = sourceConn.getMetaData();
        try (ResultSet columns = metaData.getColumns(SOURCE_DATABASE, null, SOURCE_TABLE, null)) {
            List<String> columnDefs = new ArrayList<>();
            String primaryKey = null;

            while (columns.next()) {
                String columnName = columns.getString("COLUMN_NAME");
                String dataType = columns.getString("TYPE_NAME").toUpperCase();
                int columnSize = columns.getInt("COLUMN_SIZE");
                int decimalDigits = columns.getInt("DECIMAL_DIGITS");
                boolean isNullable = columns.getInt("NULLABLE") == DatabaseMetaData.columnNullable;
                boolean isAutoIncrement = "YES".equals(columns.getString("IS_AUTOINCREMENT"));

                // 映射数据类型
                String sqlServerType = mapDataType(dataType, columnSize, decimalDigits);

                StringBuilder columnDef = new StringBuilder();
                columnDef.append("    ").append(columnName).append(" ").append(sqlServerType);

                if (!isNullable) {
                    columnDef.append(" NOT NULL");
                }

                // 如果是自增字段，设置为主键但不设置IDENTITY
                if (isAutoIncrement) {
                    primaryKey = columnName;
                }

                columnDefs.add(columnDef.toString());
            }

            sql.append(String.join(",\n", columnDefs));

            // 添加主键
            if (primaryKey != null) {
                sql.append(",\n    PRIMARY KEY (").append(primaryKey).append(")");
            }

            sql.append("\n)");
        }

        return sql.toString();
    }

    /**
     * 映射数据类型
     */
    private static String mapDataType(String mysqlType, int size, int scale) {
        String baseType = mysqlType.toUpperCase();

        // 处理带参数的类型
        if (baseType.contains("(")) {
            baseType = baseType.substring(0, baseType.indexOf("("));
        }

        String sqlServerType = TYPE_MAPPING.get(baseType);
        if (sqlServerType == null) {
            sqlServerType = "NVARCHAR(MAX)"; // 默认类型
        }

        // 处理需要长度参数的类型
        if (sqlServerType.equals("NVARCHAR") || sqlServerType.equals("CHAR")) {
            if (size > 4000) {
                return sqlServerType.equals("NVARCHAR") ? "NVARCHAR(MAX)" : "VARCHAR(MAX)";
            } else {
                return sqlServerType + "(" + size + ")";
            }
        } else if (sqlServerType.equals("DECIMAL") && scale > 0) {
            return sqlServerType + "(" + size + "," + scale + ")";
        }

        return sqlServerType;
    }

    /**
     * 获取源表总记录数
     */
    private static long getTotalRecords() {
        Connection conn = null;
        try {
            System.out.println("开始获取源表总记录数...");
            conn = createConnectionWithRetry(SOURCE_URL, SOURCE_USERNAME, SOURCE_PASSWORD, "mysql");
            if (conn == null) {
                System.err.println("❌ 无法创建数据库连接，获取记录总数失败");
                return 0;
            }

            String countSQL = "SELECT COUNT(*) FROM " + SOURCE_TABLE;
            System.out.println("执行查询: " + countSQL);
            
            try (Statement stmt = conn.createStatement()) {
                stmt.setQueryTimeout(QUERY_TIMEOUT);
                try (ResultSet rs = stmt.executeQuery(countSQL)) {
                    if (rs.next()) {
                        long count = rs.getLong(1);
                        System.out.println("✅ 获取记录总数成功: " + count);
                        return count;
                    } else {
                        System.err.println("❌ 获取记录总数失败: 查询未返回结果");
                    }
                }
            }
        } catch (SQLException e) {
            System.err.println("❌ 获取记录总数失败: " + e.getMessage());
            e.printStackTrace();
        } finally {
            if (conn != null) try { conn.close(); } catch (SQLException ignored) {}
        }
        return 0;
    }

    /**
     * 启动进度监控线程
     */
    private static void startProgressMonitor() {
        Thread progressThread = new Thread(() -> {
            while (processedRecords.get() < totalRecords.get()) {
                try {
                    Thread.sleep(10000); // 每10秒更新一次进度
                    printProgress();
                } catch (InterruptedException e) {
                    break;
                }
            }
        });
        progressThread.setDaemon(true);
        progressThread.start();
    }

    /**
     * 打印同步进度
     */
    private static void printProgress() {
        long processed = processedRecords.get();
        long total = totalRecords.get();
        long failed = failedRecords.get();
        long elapsed = System.currentTimeMillis() - startTime;

        double progress = (double) processed / total * 100;
        double speed = processed / (elapsed / 1000.0);
        long remaining = total - processed;
        long eta = speed > 0 ? (long) (remaining / speed) : 0;

        System.out.printf("📈 进度: %.2f%% (%d/%d) | 速度: %.0f记录/秒 | 失败: %d | 预计剩余: %s\n",
                progress, processed, total, speed, failed, formatTime(eta));
    }

    /**
     * 格式化时间
     */
    private static String formatTime(long seconds) {
        if (seconds < 60) return seconds + "秒";
        if (seconds < 3600) return (seconds / 60) + "分" + (seconds % 60) + "秒";
        return (seconds / 3600) + "时" + ((seconds % 3600) / 60) + "分";
    }

    /**
     * 执行同步
     */
    private static void performSync(long totalRecords) {
        System.out.println("\n🚀 开始多线程同步...");

        ExecutorService executor = Executors.newFixedThreadPool(THREAD_COUNT);
        List<Future<SyncResult>> futures = new ArrayList<>();

        // 设置每个批次的大小为2000条记录
        final long BATCH_SIZE_PER_QUERY = 2000;
        
        // 计算每个线程处理的记录数
        long recordsPerThread = (totalRecords + THREAD_COUNT - 1) / THREAD_COUNT;
        System.out.println("📊 总记录数: " + totalRecords + ", 线程数: " + THREAD_COUNT + ", 每线程记录数: " + recordsPerThread);

        for (long offset = 0; offset < totalRecords; offset += recordsPerThread) {
            long limit = Math.min(recordsPerThread, totalRecords - offset);
            System.out.println("🧵 创建同步任务: offset=" + offset + ", limit=" + limit);
            SyncTask task = new SyncTask(offset, limit, BATCH_SIZE_PER_QUERY);
            futures.add(executor.submit(task));
        }

        int completedTasks = 0;
        int totalTasks = futures.size();

        for (Future<SyncResult> future : futures) {
            try {
                SyncResult result = future.get();
                completedTasks++;
                System.out.printf("✅ 任务 %d/%d 完成: 成功 %d, 失败 %d\n",
                        completedTasks, totalTasks, result.successCount, result.failureCount);
            } catch (Exception e) {
                System.err.println("❌ 任务执行失败: " + e.getMessage());
                failedRecords.addAndGet(recordsPerThread);
            }
        }

        executor.shutdown();
        printFinalStatistics();
    }

    /**
     * 打印最终统计信息
     */
    private static void printFinalStatistics() {
        long endTime = System.currentTimeMillis();
        long totalTime = endTime - startTime;
        long processed = processedRecords.get();
        long failed = failedRecords.get();

        String separator = String.join("", Collections.nCopies(60, "="));
        System.out.println("\n" + separator);
        System.out.println("📊 同步完成统计");
        System.out.println(separator);
        System.out.println("总记录数: " + totalRecords.get());
        System.out.println("成功同步: " + processed);
        System.out.println("失败记录: " + failed);
        System.out.println("成功率: " + String.format("%.2f%%", (double) processed / totalRecords.get() * 100));
        System.out.println("总耗时: " + formatTime(totalTime / 1000));
        System.out.println("平均速度: " + String.format("%.0f 记录/秒", processed / (totalTime / 1000.0)));

        if (failed == 0) {
            System.out.println("🎉 所有数据同步成功！");
        } else {
            System.out.println("⚠️ 部分数据同步失败，建议重新运行失败的批次");
        }
    }

    /**
     * 数据同步任务
     */
    static class SyncTask implements Callable<SyncResult> {
        private final long offset;
        private final long limit;
        private final long batchSizePerQuery;
        private List<String> cachedColumns;
        private String cachedInsertSQL;
        private String cachedOrderByColumn;

        public SyncTask(long offset, long limit, long batchSizePerQuery) {
            this.offset = offset;
            this.limit = limit;
            this.batchSizePerQuery = batchSizePerQuery;
        }

        @Override
        public SyncResult call() {
            SyncResult result = new SyncResult();
            Connection sourceConn = null;
            Connection targetConn = null;

            System.out.println("开始执行同步任务: offset=" + offset + ", limit=" + limit);
            
            try {
                sourceConn = createConnectionWithRetry(SOURCE_URL, SOURCE_USERNAME, SOURCE_PASSWORD, "mysql");
                targetConn = createConnectionWithRetry(TARGET_URL, TARGET_USERNAME, TARGET_PASSWORD, "sqlserver");

                if (sourceConn == null || targetConn == null) {
                    System.err.println("❌ 无法创建数据库连接，任务失败: offset=" + offset + ", limit=" + limit);
                    result.failureCount = limit;
                    failedRecords.addAndGet(limit);
                    return result;
                }

                // 获取列信息（只获取一次）
                if (cachedColumns == null) {
                    cachedColumns = getColumnNames(sourceConn);
                    cachedInsertSQL = buildInsertSQL(cachedColumns);
                    cachedOrderByColumn = getOrderByColumn(sourceConn);
                }

                System.out.println("开始处理数据: 范围=[" + offset + "-" + (offset + limit) + "]");
                
                // 将大数据段分成多个小批次处理
                long totalProcessed = 0;
                long remainingLimit = limit;
                
                while (totalProcessed < limit) {
                    // 计算当前批次的大小
                    long currentBatchSize = Math.min(batchSizePerQuery, remainingLimit);
                    long currentOffset = offset + totalProcessed;
                    
                    System.out.println("处理小批次: [" + currentOffset + "-" + (currentOffset + currentBatchSize) + "] (总进度: " + totalProcessed + "/" + limit + ")");
                    
                    boolean success = false;
                    for (int retry = 0; retry < MAX_RETRY_COUNT && !success; retry++) {
                        try {
                            // 验证并重新创建连接（如果需要）
                            sourceConn = validateAndRecreateConnection(sourceConn, SOURCE_URL, SOURCE_USERNAME, SOURCE_PASSWORD, "mysql");
                            targetConn = validateAndRecreateConnection(targetConn, TARGET_URL, TARGET_USERNAME, TARGET_PASSWORD, "sqlserver");

                            if (sourceConn == null || targetConn == null) {
                                throw new SQLException("无法建立数据库连接");
                            }

                            // 同步当前小批次
                            int syncCount = syncBatch(sourceConn, targetConn, currentOffset, currentBatchSize);
                            result.successCount += syncCount;
                            processedRecords.addAndGet(syncCount);
                            targetConn.commit(); // 提交事务
                            success = true;

                        } catch (SQLException e) {
                            try {
                                if (targetConn != null && !targetConn.isClosed()) {
                                    targetConn.rollback(); // 回滚事务
                                }
                            } catch (SQLException ignored) {}

                            System.err.println("❌ 批次同步失败 [" + currentOffset + "-" + (currentOffset + currentBatchSize) + "] (尝试" + (retry + 1) + "次): " + e.getMessage());

                            if (retry == MAX_RETRY_COUNT - 1) {
                                result.failureCount += currentBatchSize;
                                failedRecords.addAndGet(currentBatchSize);
                            } else {
                                try {
                                    // 指数退避策略
                                    long delay = CONNECTION_RETRY_DELAY * (long)Math.pow(1.5, retry);
                                    System.out.println("⏱️ 等待" + (delay / 1000) + "秒后重试批次 [" + currentOffset + "-" + (currentOffset + currentBatchSize) + "]...");
                                    Thread.sleep(delay);
                                } catch (InterruptedException ie) {
                                    Thread.currentThread().interrupt();
                                    break;
                                }
                            }
                        }
                    }
                    
                    // 更新进度
                    totalProcessed += currentBatchSize;
                    remainingLimit -= currentBatchSize;
                }

            } catch (Exception e) {
                result.failureCount += limit;
                failedRecords.addAndGet(limit);
                System.err.println("❌ 同步任务失败: " + e.getMessage());
                e.printStackTrace();
            } finally {
                if (sourceConn != null) try { sourceConn.close(); } catch (SQLException ignored) {}
                if (targetConn != null) try { targetConn.close(); } catch (SQLException ignored) {}
            }

            return result;
        }

        /**
         * 获取列名列表
         */
        private List<String> getColumnNames(Connection conn) throws SQLException {
            List<String> columns = new ArrayList<>();
            DatabaseMetaData metaData = conn.getMetaData();
            try (ResultSet rs = metaData.getColumns(SOURCE_DATABASE, null, SOURCE_TABLE, null)) {
                while (rs.next()) {
                    String columnName = rs.getString("COLUMN_NAME");
                    columns.add(columnName);
                }
            }
            return columns;
        }

        /**
         * 获取排序字段，优先使用主键，否则使用第一个字段
         */
        private String getOrderByColumn(Connection conn) throws SQLException {
            // 先尝试获取主键
            DatabaseMetaData metaData = conn.getMetaData();
            try (ResultSet rs = metaData.getPrimaryKeys(SOURCE_DATABASE, null, SOURCE_TABLE)) {
                if (rs.next()) {
                    String primaryKey = rs.getString("COLUMN_NAME");
                    System.out.println("使用主键作为排序字段: " + primaryKey);
                    return primaryKey;
                }
            }

            // 如果没有主键，使用第一个字段
            if (cachedColumns != null && !cachedColumns.isEmpty()) {
                String firstColumn = cachedColumns.get(0);
                System.out.println("使用第一个字段作为排序字段: " + firstColumn);
                return firstColumn;
            }

            // 如果都没有，默认使用id
            System.out.println("使用默认字段id作为排序字段");
            return "id";
        }

        /**
         * 构建插入SQL语句
         */
        private String buildInsertSQL(List<String> columns) {
            String columnList = String.join(", ", columns);
            String valueList = String.join(", ", Collections.nCopies(columns.size(), "?"));
            return "INSERT INTO " + TARGET_TABLE + " (" + columnList + ") VALUES (" + valueList + ")";
        }

        /**
         * 同步一个批次的数据
         */
        private int syncBatch(Connection sourceConn, Connection targetConn, long offset, long limit) throws SQLException {
            // 使用动态获取的排序字段确保查询结果的确定性
            String selectSQL = "SELECT " + String.join(", ", cachedColumns) +
                    " FROM " + SOURCE_TABLE + " ORDER BY " + cachedOrderByColumn + " LIMIT " + offset + ", " + limit;
            
            System.out.println("执行查询: " + selectSQL);

            try (Statement selectStmt = sourceConn.createStatement();
                 ResultSet rs = selectStmt.executeQuery(selectSQL);
                 PreparedStatement insertStmt = targetConn.prepareStatement(cachedInsertSQL)) {

                // 设置查询和插入超时
                selectStmt.setQueryTimeout(QUERY_TIMEOUT);
                insertStmt.setQueryTimeout(QUERY_TIMEOUT);

                int batchCount = 0;
                int totalProcessed = 0;
                int subBatchSize = 100; // 子批次大小，减少内存压力
                
                System.out.println("开始处理批次数据: offset=" + offset + ", limit=" + limit);
                boolean hasData = false;
                
                while (rs.next()) {
                    hasData = true;
                    for (int i = 0; i < cachedColumns.size(); i++) {
                        Object value = rs.getObject(i + 1);
                        insertStmt.setObject(i + 1, value);
                    }
                    insertStmt.addBatch();
                    batchCount++;
                    totalProcessed++;

                    if (batchCount >= subBatchSize) {
                        insertStmt.executeBatch();
                        insertStmt.clearBatch();
                        batchCount = 0;
                    }
                }

                if (batchCount > 0) {
                    insertStmt.executeBatch();
                }
                
                if (!hasData) {
                    System.out.println("⚠️ 查询未返回任何数据: offset=" + offset + ", limit=" + limit);
                } else {
                    System.out.println("✅ 批次处理完成: offset=" + offset + ", limit=" + limit + ", 实际处理=" + totalProcessed);
                }

                return totalProcessed;
            }
        }
    }

    /**
     * 同步结果类
     */
    static class SyncResult {
        long successCount = 0;
        long failureCount = 0;
    }
}