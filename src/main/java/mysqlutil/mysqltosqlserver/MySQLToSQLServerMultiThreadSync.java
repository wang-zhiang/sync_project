package mysqlutil.mysqltosqlserver;

import java.sql.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * MySQL到SQLServer多线程高可靠全量同步工具
 */
public class MySQLToSQLServerMultiThreadSync {
    // ================== 参数配置 ==================
    // MySQL配置
    private static final String MYSQL_HOST = "101.89.122.158";
    private static final String MYSQL_DATABASE = "xhs_app";
    private static final String MYSQL_TABLE = "xhs_set_spider_25_11_31";
    private static final String MYSQL_URL = "jdbc:mysql://" + MYSQL_HOST + ":3306/" + MYSQL_DATABASE
            + "?useSSL=false&serverTimezone=UTC&allowPublicKeyRetrieval=true&rewriteBatchedStatements=true"
            + "&connectTimeout=30000&socketTimeout=120000&autoReconnect=true&failOverReadOnly=false"
            + "&maxReconnects=5&initialTimeout=2&useUnicode=true&characterEncoding=utf8";
    private static final String MYSQL_USERNAME = "root";
    private static final String MYSQL_PASSWORD = "smartpath@123";

    // SQLServer配置
   // private static final String SQLSERVER_HOST = "192.168.4.39";
    private static final String SQLSERVER_HOST = "192.168.3.183";
    private static final String SQLSERVER_DATABASE = "Trading_RedBook";
    private static final String SQLSERVER_TABLE = "xhs_set_spider_25_11_31";
    private static final String SQLSERVER_URL = "jdbc:sqlserver://" + SQLSERVER_HOST + ";databaseName=" + SQLSERVER_DATABASE
            + ";trustServerCertificate=true;connectTimeout=30000;socketTimeout=1200000;loginTimeout=30000";
    private static final String SQLSERVER_USERNAME = "sa";
    private static final String SQLSERVER_PASSWORD = "smartpthdata";

    // 同步配置
    private static final int THREAD_COUNT = 5;
    private static final int BATCH_SIZE = 5000;
    private static final int MAX_RETRY = 8;
    private static final int RETRY_DELAY_MS = 8000;
    private static final int QUERY_TIMEOUT = 300;
    private static final int SUB_BATCH_SIZE = 100;

    // 进度监控
    private static AtomicLong processedRecords = new AtomicLong(0);
    private static AtomicLong totalRecords = new AtomicLong(0);
    private static AtomicLong failedRecords = new AtomicLong(0);
    private static long startTime;

    // 类型映射
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
        System.out.println("=== MySQL到SQLServer多线程高可靠全量同步工具 ===");
        System.out.println("源数据库: " + MYSQL_HOST + "/" + MYSQL_DATABASE + "." + MYSQL_TABLE);
        System.out.println("目标数据库: " + SQLSERVER_HOST + "/" + SQLSERVER_DATABASE + "." + SQLSERVER_TABLE);
        System.out.println("线程数量: " + THREAD_COUNT);
        System.out.println("批处理大小: " + BATCH_SIZE);

        startTime = System.currentTimeMillis();

        try {
            if (!testConnections()) {
                System.err.println("❌ 数据库连接测试失败，程序退出");
                return;
            }
            if (!ensureTargetTableExists()) {
                System.err.println("❌ 目标表创建失败，程序退出");
                return;
            }
            long total = getTotalRecords();
            if (total <= 0) {
                System.out.println("❌ 源表没有数据或获取记录数失败");
                return;
            }
            totalRecords.set(total);
            System.out.println("📊 源表总记录数: " + total);
            startProgressMonitor();
            performSync(total);
        } catch (Exception e) {
            System.err.println("❌ 同步过程中发生错误: " + e.getMessage());
            e.printStackTrace();
        }
    }

    // ========== 连接管理 ==========
    private static Connection createConnectionWithRetry(String url, String username, String password, String dbType) {
        for (int retry = 0; retry < MAX_RETRY; retry++) {
            try {
                Connection conn = DriverManager.getConnection(url, username, password);
                if ("mysql".equals(dbType)) {
                    conn.setAutoCommit(true);
                    try (Statement stmt = conn.createStatement()) {
                        stmt.execute("SET SESSION wait_timeout = 7200");
                        stmt.execute("SET SESSION interactive_timeout = 7200");
                        stmt.execute("SET SESSION net_read_timeout = 300");
                        stmt.execute("SET SESSION net_write_timeout = 300");
                    }
                } else if ("sqlserver".equals(dbType)) {
                    conn.setAutoCommit(false);
                }
                System.out.println("✅ " + dbType + "连接成功 (尝试" + (retry + 1) + "次)");
                return conn;
            } catch (SQLException e) {
                System.err.println("❌ " + dbType + "连接失败 (尝试" + (retry + 1) + "次): " + e.getMessage());
                if (retry < MAX_RETRY - 1) {
                    try { Thread.sleep(RETRY_DELAY_MS * (retry + 1)); } catch (InterruptedException ie) { Thread.currentThread().interrupt(); break; }
                }
            }
        }
        return null;
    }

    private static Connection validateAndRecreateConnection(Connection conn, String url, String username, String password, String dbType) {
        try {
            if (conn == null || conn.isClosed() || !conn.isValid(5)) {
                if (conn != null) { try { conn.close(); } catch (SQLException ignored) {} }
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

    private static boolean testConnections() {
        System.out.println("\n🔗 测试数据库连接...");
        Connection mysqlConn = createConnectionWithRetry(MYSQL_URL, MYSQL_USERNAME, MYSQL_PASSWORD, "mysql");
        if (mysqlConn == null) return false;
        try { mysqlConn.close(); } catch (SQLException ignored) {}
        Connection sqlServerConn = createConnectionWithRetry(SQLSERVER_URL, SQLSERVER_USERNAME, SQLSERVER_PASSWORD, "sqlserver");
        if (sqlServerConn == null) return false;
        try { sqlServerConn.close(); } catch (SQLException ignored) {}
        return true;
    }

    // ========== 建表逻辑 ==========
    private static boolean ensureTargetTableExists() {
        System.out.println("\n🏗️ 检查目标表...");
        Connection sourceConn = null;
        Connection targetConn = null;
        try {
            sourceConn = createConnectionWithRetry(MYSQL_URL, MYSQL_USERNAME, MYSQL_PASSWORD, "mysql");
            targetConn = createConnectionWithRetry(SQLSERVER_URL, SQLSERVER_USERNAME, SQLSERVER_PASSWORD, "sqlserver");
            if (sourceConn == null || targetConn == null) return false;
            if (tableExists(targetConn, SQLSERVER_TABLE)) {
                System.out.println("✅ 目标表已存在: " + SQLSERVER_TABLE);
                return true;
            }
            System.out.println("📋 获取源表结构...");
            String createTableSQL = generateCreateTableSQL(sourceConn);
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

    private static boolean tableExists(Connection conn, String tableName) throws SQLException {
        DatabaseMetaData metaData = conn.getMetaData();
        try (ResultSet rs = metaData.getTables(null, null, tableName, new String[]{"TABLE"})) {
            return rs.next();
        }
    }

    private static String generateCreateTableSQL(Connection sourceConn) throws SQLException {
        StringBuilder sql = new StringBuilder();
        sql.append("CREATE TABLE ").append(SQLSERVER_TABLE).append(" (\n");
        DatabaseMetaData metaData = sourceConn.getMetaData();
        try (ResultSet columns = metaData.getColumns(MYSQL_DATABASE, null, MYSQL_TABLE, null)) {
            List<String> columnDefs = new ArrayList<>();
            String primaryKey = null;
            while (columns.next()) {
                String columnName = columns.getString("COLUMN_NAME");
                String dataType = columns.getString("TYPE_NAME").toUpperCase();
                int columnSize = columns.getInt("COLUMN_SIZE");
                int decimalDigits = columns.getInt("DECIMAL_DIGITS");
                boolean isNullable = columns.getInt("NULLABLE") == DatabaseMetaData.columnNullable;
                boolean isAutoIncrement = "YES".equals(columns.getString("IS_AUTOINCREMENT"));
                String sqlServerType = mapDataType(dataType, columnSize, decimalDigits);
                StringBuilder columnDef = new StringBuilder();
                columnDef.append("    ").append(columnName).append(" ").append(sqlServerType);
                if (!isNullable) {
                    columnDef.append(" NOT NULL");
                }
                if (isAutoIncrement) {
                    primaryKey = columnName;
                }
                columnDefs.add(columnDef.toString());
            }
            sql.append(String.join(",\n", columnDefs));
            if (primaryKey != null) {
                sql.append(",\n    PRIMARY KEY (").append(primaryKey).append(")");
            }
            sql.append("\n)");
        }
        return sql.toString();
    }

    private static String mapDataType(String mysqlType, int size, int scale) {
        String baseType = mysqlType.toUpperCase();
        if (baseType.contains("(")) {
            baseType = baseType.substring(0, baseType.indexOf("("));
        }
        String sqlServerType = TYPE_MAPPING.get(baseType);
        if (sqlServerType == null) {
            sqlServerType = "NVARCHAR(MAX)";
        }
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

    // ========== 进度监控 ==========
    private static void startProgressMonitor() {
        Thread progressThread = new Thread(() -> {
            while (processedRecords.get() < totalRecords.get()) {
                try {
                    Thread.sleep(10000);
                    printProgress();
                } catch (InterruptedException e) {
                    break;
                }
            }
        });
        progressThread.setDaemon(true);
        progressThread.start();
    }

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

    private static String formatTime(long seconds) {
        if (seconds < 60) return seconds + "秒";
        if (seconds < 3600) return (seconds / 60) + "分" + (seconds % 60) + "秒";
        return (seconds / 3600) + "时" + ((seconds % 3600) / 60) + "分";
    }

    // ========== 获取总记录数 ==========
    private static long getTotalRecords() {
        Connection conn = null;
        try {
            conn = createConnectionWithRetry(MYSQL_URL, MYSQL_USERNAME, MYSQL_PASSWORD, "mysql");
            if (conn == null) return 0;
            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM " + MYSQL_TABLE)) {
                if (rs.next()) {
                    return rs.getLong(1);
                }
            }
        } catch (SQLException e) {
            System.err.println("❌ 获取记录总数失败: " + e.getMessage());
        } finally {
            if (conn != null) try { conn.close(); } catch (SQLException ignored) {}
        }
        return 0;
    }

    // ========== 多线程同步主流程 ==========
    private static void performSync(long totalRecords) {
        System.out.println("\n🚀 开始多线程同步...");
        ExecutorService executor = Executors.newFixedThreadPool(THREAD_COUNT);
        List<Future<SyncResult>> futures = new ArrayList<>();
        long recordsPerThread = (totalRecords + THREAD_COUNT - 1) / THREAD_COUNT;
        for (long offset = 0; offset < totalRecords; offset += recordsPerThread) {
            long limit = Math.min(recordsPerThread, totalRecords - offset);
            SyncTask task = new SyncTask(offset, limit);
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

    // ========== 同步任务实现 ==========
    static class SyncTask implements Callable<SyncResult> {
        private final long offset;
        private final long limit;
        private List<String> cachedColumns;
        private String cachedInsertSQL;

        public SyncTask(long offset, long limit) {
            this.offset = offset;
            this.limit = limit;
        }

        @Override
        public SyncResult call() {
            SyncResult result = new SyncResult();
            Connection sourceConn = null;
            Connection targetConn = null;
            try {
                sourceConn = createConnectionWithRetry(MYSQL_URL, MYSQL_USERNAME, MYSQL_PASSWORD, "mysql");
                targetConn = createConnectionWithRetry(SQLSERVER_URL, SQLSERVER_USERNAME, SQLSERVER_PASSWORD, "sqlserver");
                if (sourceConn == null || targetConn == null) {
                    result.failureCount = limit;
                    failedRecords.addAndGet(limit);
                    return result;
                }
                if (cachedColumns == null) {
                    cachedColumns = getColumnNames(sourceConn);
                    cachedInsertSQL = buildInsertSQL(cachedColumns);
                }
                for (long currentOffset = offset; currentOffset < offset + limit; currentOffset += BATCH_SIZE) {
                    long currentLimit = Math.min(BATCH_SIZE, offset + limit - currentOffset);
                    boolean success = false;
                    for (int retry = 0; retry < MAX_RETRY && !success; retry++) {
                        List<List<Object>> failedRows = new ArrayList<>();
                        try {
                            sourceConn = validateAndRecreateConnection(sourceConn, MYSQL_URL, MYSQL_USERNAME, MYSQL_PASSWORD, "mysql");
                            targetConn = validateAndRecreateConnection(targetConn, SQLSERVER_URL, SQLSERVER_USERNAME, SQLSERVER_PASSWORD, "sqlserver");
                            if (sourceConn == null || targetConn == null) {
                                throw new SQLException("无法建立数据库连接");
                            }
                            int syncCount = syncBatch(sourceConn, targetConn, currentOffset, currentLimit, failedRows);
                            result.successCount += syncCount;
                            processedRecords.addAndGet(syncCount);
                            targetConn.commit();
                            if (!failedRows.isEmpty()) {
                                result.failureCount += failedRows.size();
                                failedRecords.addAndGet(failedRows.size());
                                System.err.println("❌ 批次同步部分失败，失败行如下：");
                                for (List<Object> row : failedRows) {
                                    System.err.println(row);
                                }
                            }
                            success = failedRows.isEmpty();
                        } catch (SQLException e) {
                            try { if (targetConn != null) targetConn.rollback(); } catch (SQLException ignored) {}
                            System.err.println("❌ 批次同步失败 [" + currentOffset + "-" + (currentOffset + currentLimit) + "] (尝试" + (retry + 1) + "次): " + e.getMessage());
                            if (retry == MAX_RETRY - 1) {
                                result.failureCount += currentLimit;
                                failedRecords.addAndGet(currentLimit);
                                System.err.println("❌ 最终失败批次区间: [" + currentOffset + ", " + (currentOffset + currentLimit) + "]");
                            } else {
                                try { Thread.sleep(RETRY_DELAY_MS * (retry + 1)); } catch (InterruptedException ie) { Thread.currentThread().interrupt(); break; }
                            }
                        }
                    }
                }
            } catch (Exception e) {
                result.failureCount += limit;
                failedRecords.addAndGet(limit);
                System.err.println("❌ 同步任务失败: " + e.getMessage());
            } finally {
                if (sourceConn != null) try { sourceConn.close(); } catch (SQLException ignored) {}
                if (targetConn != null) try { targetConn.close(); } catch (SQLException ignored) {}
            }
            return result;
        }

        private List<String> getColumnNames(Connection conn) throws SQLException {
            List<String> columns = new ArrayList<>();
            DatabaseMetaData metaData = conn.getMetaData();
            try (ResultSet rs = metaData.getColumns(MYSQL_DATABASE, null, MYSQL_TABLE, null)) {
                while (rs.next()) {
                    String columnName = rs.getString("COLUMN_NAME");
                    columns.add(columnName);
                }
            }
            return columns;
        }

        private String buildInsertSQL(List<String> columns) {
            String columnList = String.join(", ", columns);
            String valueList = String.join(", ", Collections.nCopies(columns.size(), "?"));
            return "INSERT INTO " + SQLSERVER_TABLE + " (" + columnList + ") VALUES (" + valueList + ")";
        }

        private int syncBatch(Connection sourceConn, Connection targetConn, long offset, long limit, List<List<Object>> failedRows) throws SQLException {
            String selectSQL = "SELECT " + String.join(", ", cachedColumns) +
                    " FROM " + MYSQL_TABLE + " LIMIT " + offset + ", " + limit;
            try (Statement selectStmt = sourceConn.createStatement();
                 ResultSet rs = selectStmt.executeQuery(selectSQL);
                 PreparedStatement insertStmt = targetConn.prepareStatement(cachedInsertSQL)) {
                selectStmt.setQueryTimeout(QUERY_TIMEOUT);
                insertStmt.setQueryTimeout(QUERY_TIMEOUT);
                int batchCount = 0;
                int totalProcessed = 0;
                List<List<Object>> batchRows = new ArrayList<>();
                while (rs.next()) {
                    List<Object> row = new ArrayList<>();
                    for (int i = 0; i < cachedColumns.size(); i++) {
                        Object value = rs.getObject(i + 1);
                        insertStmt.setObject(i + 1, value);
                        row.add(value);
                    }
                    insertStmt.addBatch();
                    batchRows.add(row);
                    batchCount++;
                    totalProcessed++;
                    if (batchCount >= SUB_BATCH_SIZE) {
                        handleBatch(insertStmt, batchRows, failedRows);
                        batchRows.clear();
                        batchCount = 0;
                    }
                }
                if (batchCount > 0) {
                    handleBatch(insertStmt, batchRows, failedRows);
                }
                return totalProcessed - failedRows.size();
            }
        }

        private void handleBatch(PreparedStatement insertStmt, List<List<Object>> batchRows, List<List<Object>> failedRows) {
            try {
                int[] results = insertStmt.executeBatch();
                for (int i = 0; i < results.length; i++) {
                    if (results[i] == Statement.EXECUTE_FAILED) {
                        failedRows.add(batchRows.get(i));
                    }
                }
            } catch (BatchUpdateException bue) {
                int[] results = bue.getUpdateCounts();
                for (int i = 0; i < results.length; i++) {
                    if (results[i] == Statement.EXECUTE_FAILED) {
                        failedRows.add(batchRows.get(i));
                    }
                }
                for (int i = results.length; i < batchRows.size(); i++) {
                    failedRows.add(batchRows.get(i));
                }
            } catch (SQLException e) {
                failedRows.addAll(batchRows);
            }
        }
    }

    static class SyncResult {
        long successCount = 0;
        long failureCount = 0;
    }
} 