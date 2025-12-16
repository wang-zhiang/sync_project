package cktosqlserverutil.cksqlserver;

import java.sql.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class ClickHouseToSqlServerSync2 {

    // ================= 配置区域 (请在此修改) =================
    // 1. ClickHouse 配置
    private static final String CH_URL = "jdbc:clickhouse://192.168.1.100:8123/default";
    private static final String CH_USER = "default";
    private static final String CH_PASSWORD = "";

    // 2. SQL Server 配置
    // encrypt=false;trustServerCertificate=true 是为了防止 SSL 报错
    private static final String MSSQL_URL = "jdbc:sqlserver://192.168.1.200:1433;databaseName=TestDB;encrypt=false;trustServerCertificate=true";
    private static final String MSSQL_USER = "sa";
    private static final String MSSQL_PASSWORD = "your_password";

    // 3. 同步任务配置
    // 源数据查询 (支持自定义 WHERE/JOIN，不要加分号，不要加 LIMIT)
    private static final String SOURCE_SQL = "SELECT * FROM user_analytics WHERE age > 0";
    // 目标表名
    private static final String TARGET_TABLE = "dbo.sync_user_analytics";

    // 模式选择: OVERWRITE (删除重建表) 或 APPEND (追加写入)
    private static final SyncMode CURRENT_MODE = SyncMode.OVERWRITE;

    // 性能配置
    private static final int THREAD_COUNT = 4;      // 线程数
    private static final int BATCH_SIZE = 3000;     // 批量提交大小

    // =======================================================

    // 全局控制
    private static final AtomicBoolean isRunning = new AtomicBoolean(true);
    private static final AtomicBoolean hasError = new AtomicBoolean(false);
    private static final AtomicLong totalSyncedRows = new AtomicLong(0);
    private static long totalSourceRows = 0;

    public enum SyncMode { OVERWRITE, APPEND }

    public static void main(String[] args) {
        long startTime = System.currentTimeMillis();
        ExecutorService executor = null;

        try {
            System.out.println("=== 🚀 开始 ClickHouse 到 SQL Server 同步任务 ===");
            System.out.println("模式: " + CURRENT_MODE);
            System.out.println("线程数: " + THREAD_COUNT);

            // 1. 预检查 & 准备表结构
            prepareTargetTable();

            // 2. 获取源数据总条数 (用于计算进度)
            totalSourceRows = getSourceCount();
            System.out.println("源数据预估总行数: " + totalSourceRows);
            if (totalSourceRows == 0) {
                System.out.println("源数据为空，任务结束。");
                return;
            }

            // 3. 启动监控线程
            startMonitorThread(startTime);

            // 4. 启动工作线程
            executor = Executors.newFixedThreadPool(THREAD_COUNT);
            CountDownLatch latch = new CountDownLatch(THREAD_COUNT);

            for (int i = 0; i < THREAD_COUNT; i++) {
                executor.submit(new WorkerTask(i, THREAD_COUNT, latch));
            }

            // 等待所有任务完成
            latch.await();

            if (hasError.get()) {
                System.err.println("\n❌ 任务失败！检测到某个线程发生异常，已触发全局停止。");
                System.err.println("提示：请检查日志，修复后建议使用 OVERWRITE 模式重试。");
            } else {
                long endTime = System.currentTimeMillis();
                System.out.println("\n✅ 任务全部完成！");
                System.out.println("总耗时: " + (endTime - startTime) / 1000 + " 秒");
                System.out.println("总同步行数: " + totalSyncedRows.get());
            }

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (executor != null) executor.shutdownNow();
        }
    }

    /**
     * 准备目标表：如果是 OVERWRITE，则删除并重建；如果是 APPEND，检查表是否存在。
     */
    private static void prepareTargetTable() throws Exception {
        try (Connection chConn = DriverManager.getConnection(CH_URL, CH_USER, CH_PASSWORD);
             Connection sqlConn = DriverManager.getConnection(MSSQL_URL, MSSQL_USER, MSSQL_PASSWORD);
             Statement chStmt = chConn.createStatement();
             Statement sqlStmt = sqlConn.createStatement()) {

            // 获取 ClickHouse 元数据 (取0条数据，只为了拿结构)
            String metaSql = "SELECT * FROM (" + SOURCE_SQL + ") LIMIT 0";
            ResultSet chRs = chStmt.executeQuery(metaSql);
            ResultSetMetaData metaData = chRs.getMetaData();

            if (CURRENT_MODE == SyncMode.OVERWRITE) {
                System.out.println("正在执行表重置操作 (OVERWRITE)...");
                try {
                    sqlStmt.execute("DROP TABLE " + TARGET_TABLE);
                } catch (Exception ignored) {
                    // 忽略表不存在的错误
                }

                // 生成 CREATE TABLE 语句
                StringBuilder createSql = new StringBuilder("CREATE TABLE " + TARGET_TABLE + " (");
                int colCount = metaData.getColumnCount();
                for (int i = 1; i <= colCount; i++) {
                    String colName = metaData.getColumnName(i);
                    String colType = mapDataType(metaData, i); // 使用更新后的映射逻辑
                    createSql.append("[").append(colName).append("] ").append(colType);
                    if (i < colCount) {
                        createSql.append(", ");
                    }
                }
                createSql.append(")");

                System.out.println("生成建表语句: " + createSql);
                sqlStmt.execute(createSql.toString());
                System.out.println("目标表已创建。");
            } else {
                System.out.println("APPEND模式：跳过建表，直接写入现有表。");
            }
        }
    }

    /**
     * 类型映射器：根据你的需求定制
     */
    private static String mapDataType(ResultSetMetaData meta, int index) throws SQLException {
        // 获取类型名并转为小写，方便统一匹配
        // meta.getColumnTypeName 可能返回 "Nullable(Int32)" 或 "String" 等
        String chType = meta.getColumnTypeName(index).toLowerCase();
        int precision = meta.getPrecision(index);
        int scale = meta.getScale(index);

        // 按照你提供的逻辑进行匹配
        if (chType.contains("string") || chType.contains("fixedstring")) {
            return "NVARCHAR(MAX)";
        } else if (chType.contains("int8")) {
            // 注意：SQL Server TINYINT 是 0-255 (无符号)，如果 CH Int8 有负数，可能会报错，需改为 SMALLINT
            return "TINYINT";
        } else if (chType.contains("int16")) {
            return "SMALLINT";
        } else if (chType.contains("int32")) {
            return "INT";
        } else if (chType.contains("int64")) {
            return "BIGINT";
        } else if (chType.contains("float32")) {
            return "REAL";
        } else if (chType.contains("float64")) {
            return "FLOAT";
        } else if (chType.contains("decimal")) {
            // 如果 CH 的 decimal 精度定义明确，也可以尝试用: "DECIMAL(" + precision + "," + scale + ")";
            return "DECIMAL(18,8)";
        } else if (chType.contains("datetime")) {
            // 必须先判断 datetime，因为 datetime 包含了 "date" 字符串
            return "DATETIME";
        } else if (chType.contains("date")) {
            return "DATE";
        } else if (chType.contains("uuid")) {
            return "UNIQUEIDENTIFIER";
        } else {
            return "NVARCHAR(MAX)"; // 兜底默认类型
        }
    }

    /**
     * 获取源数据总条数
     */
    private static long getSourceCount() {
        try (Connection conn = DriverManager.getConnection(CH_URL, CH_USER, CH_PASSWORD);
             Statement stmt = conn.createStatement()) {
            String countSql = "SELECT count() FROM (" + SOURCE_SQL + ")";
            ResultSet rs = stmt.executeQuery(countSql);
            if (rs.next()) {
                return rs.getLong(1);
            }
        } catch (Exception e) {
            System.err.println("无法获取总行数: " + e.getMessage());
        }
        return 0;
    }

    /**
     * 监控线程：打印进度
     */
    private static void startMonitorThread(long startTime) {
        Thread monitor = new Thread(() -> {
            while (isRunning.get() && !Thread.currentThread().isInterrupted()) {
                try {
                    Thread.sleep(2000);
                    long synced = totalSyncedRows.get();
                    double percent = totalSourceRows > 0 ? (synced * 100.0 / totalSourceRows) : 0;
                    long seconds = (System.currentTimeMillis() - startTime) / 1000;
                    long speed = seconds > 0 ? synced / seconds : 0;

                    System.out.printf("\r[同步进度] %.2f%% (%d / %d) | 速度: %d 行/秒 | 耗时: %ds",
                            percent, synced, totalSourceRows, speed, seconds);

                    if (synced >= totalSourceRows && totalSourceRows > 0) break;
                } catch (InterruptedException e) {
                    break;
                }
            }
        });
        monitor.setDaemon(true);
        monitor.start();
    }

    /**
     * 工作线程任务类
     */
    static class WorkerTask implements Runnable {
        private final int threadIndex;
        private final int totalThreads;
        private final CountDownLatch latch;

        public WorkerTask(int threadIndex, int totalThreads, CountDownLatch latch) {
            this.threadIndex = threadIndex;
            this.totalThreads = totalThreads;
            this.latch = latch;
        }

        @Override
        public void run() {
            // 使用 cityHash64 取模进行通用分片，无需 ID
            String splitSql = "SELECT * FROM (" + SOURCE_SQL + ") WHERE cityHash64(*) % " + totalThreads + " = " + threadIndex;

            Connection chConn = null;
            Connection sqlConn = null;
            PreparedStatement ps = null;
            ResultSet rs = null;

            try {
                chConn = DriverManager.getConnection(CH_URL, CH_USER, CH_PASSWORD);
                sqlConn = DriverManager.getConnection(MSSQL_URL, MSSQL_USER, MSSQL_PASSWORD);

                // 开启手动事务
                sqlConn.setAutoCommit(false);

                Statement chStmt = chConn.createStatement();
                chStmt.setFetchSize(BATCH_SIZE); // 流式读取

                rs = chStmt.executeQuery(splitSql);
                ResultSetMetaData meta = rs.getMetaData();
                int colCount = meta.getColumnCount();

                // 构建 Insert 语句
                StringBuilder insertSql = new StringBuilder("INSERT INTO " + TARGET_TABLE + " VALUES (");
                for (int i = 0; i < colCount; i++) {
                    insertSql.append(i == 0 ? "?" : ",?");
                }
                insertSql.append(")");

                ps = sqlConn.prepareStatement(insertSql.toString());

                int batchCount = 0;
                while (rs.next()) {
                    if (!isRunning.get()) throw new InterruptedException("收到停止信号");

                    for (int i = 1; i <= colCount; i++) {
                        Object val = rs.getObject(i);
                        // 特殊处理：ClickHouse 的 Date 可能返回 LocalDate，JDBC 驱动有时需要转换
                        // 但大多数 JDBC Driver 会自动处理，如果报错可以在这里加 instanceof 判断
                        ps.setObject(i, val);
                    }
                    ps.addBatch();
                    batchCount++;

                    if (batchCount % BATCH_SIZE == 0) {
                        ps.executeBatch();
                        sqlConn.commit();
                        totalSyncedRows.addAndGet(batchCount);
                        batchCount = 0;
                        ps.clearBatch();
                    }
                }

                // 提交剩余尾部数据
                if (batchCount > 0) {
                    ps.executeBatch();
                    sqlConn.commit();
                    totalSyncedRows.addAndGet(batchCount);
                }

            } catch (Throwable e) {
                hasError.set(true);
                isRunning.set(false); // 熔断
                System.err.printf("\n[Thread-%d] 异常: %s\n", threadIndex, e.getMessage());
                e.printStackTrace();
                try { if (sqlConn != null) sqlConn.rollback(); } catch (SQLException ex) {}
            } finally {
                try { if (rs != null) rs.close(); } catch (Exception e) {}
                try { if (ps != null) ps.close(); } catch (Exception e) {}
                try { if (chConn != null) chConn.close(); } catch (Exception e) {}
                try { if (sqlConn != null) sqlConn.close(); } catch (Exception e) {}
                latch.countDown();
            }
        }
    }
}