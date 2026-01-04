package mysqlutil.mysqltomysql;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 执行前需要注意：
 *
 * MySQL 单表-多线程并行同步工具
 * 原理：根据主键 ID 的范围 (Min ~ Max) 将数据切分成 N 份，并行同步。
 * 要求：表必须有数字类型的主键 (int/bigint)。
 */
public class MySQLSingleTableParallelSync {

    // ================= ⚙️ 配置区域 =================

    // 1. 表名
    private static final String TABLE_NAME = "supplier_order_notice";

    // 2. 主键列名 (必须是数字类型，用于切分数据)
    private static final String PRIMARY_KEY = "Id";

    // 3. 线程数量 (并发度)
    private static final int THREAD_COUNT = 5;

    // 4. 源数据库 (!!! 注意这里增加了 allowPublicKeyRetrieval=true)
//    private static final String SRC_URL = "jdbc:mysql://192.168.3.138:3306/mdlz?useUnicode=true&characterEncoding=utf-8&useSSL=false&serverTimezone=UTC&allowPublicKeyRetrieval=true";
//    private static final String SRC_USER = "root";
//    private static final String SRC_PASS = "smartpthdata";
//
//    // 5. 目标数据库 (!!! 注意这里增加了 allowPublicKeyRetrieval=true)
//    private static final String TGT_URL = "jdbc:mysql://218.78.135.17:3306/mdlz?useUnicode=true&characterEncoding=utf-8&useSSL=false&serverTimezone=UTC&rewriteBatchedStatements=true&allowPublicKeyRetrieval=true";
//    private static final String TGT_USER = "root";
//    private static final String TGT_PASS = "smartpthdata";
// 4. 源数据库 (!!! 注意这里增加了 allowPublicKeyRetrieval=true)
    private static final String SRC_URL = "jdbc:mysql://43.142.47.248:3306/hengdaproject?useUnicode=true&characterEncoding=utf-8&useSSL=false&serverTimezone=UTC&allowPublicKeyRetrieval=true";
    private static final String SRC_USER = "test";
    private static final String SRC_PASS = "YunJiBao1#";

    // 5. 目标数据库 (!!! 注意这里增加了 allowPublicKeyRetrieval=true)
    private static final String TGT_URL = "jdbc:mysql://36.213.68.80:31848/hengdaproject?useUnicode=true&characterEncoding=utf-8&useSSL=false&serverTimezone=UTC&rewriteBatchedStatements=true&allowPublicKeyRetrieval=true";
    private static final String TGT_USER = "root";
    private static final String TGT_PASS = "9yGuO74d@2025";
    // 6. 批量提交大小
    private static final int BATCH_SIZE = 2000;

    // ===============================================

    public static void main(String[] args) {
        long start = System.currentTimeMillis();
        System.out.println("🚀 [单表多线程并行同步] 启动...");
        System.out.println("🎯 目标表: " + TABLE_NAME + " | 切分主键: " + PRIMARY_KEY);

        ExecutorService executor = Executors.newFixedThreadPool(THREAD_COUNT);

        try {
            // ------------------------------------------------
            // 第一阶段：准备工作 (建表 & 获取 ID 范围)
            // ------------------------------------------------
            long minId = 0;
            long maxId = 0;

            try (Connection srcConn = DriverManager.getConnection(SRC_URL, SRC_USER, SRC_PASS);
                 Connection tgtConn = DriverManager.getConnection(TGT_URL, TGT_USER, TGT_PASS)) {

                // 1. 如果目标表不存在，自动创建
                if (!checkTableExists(tgtConn, TABLE_NAME)) {
                    System.out.println("🔨 目标表不存在，正在复制结构...");
                    String createSql = getCreateTableSql(srcConn, TABLE_NAME);
                    executeSql(tgtConn, createSql);
                } else {
                    System.out.println("♻️ 目标表已存在，将采用 INSERT IGNORE 追加数据...");
                }

                // 2. 获取源表的主键范围 (Min, Max)
                System.out.println("🔍 正在分析源表 ID 分布...");
                try (Statement stmt = srcConn.createStatement();
                     ResultSet rs = stmt.executeQuery("SELECT MIN(" + PRIMARY_KEY + "), MAX(" + PRIMARY_KEY + ") FROM " + TABLE_NAME)) {
                    if (rs.next()) {
                        minId = rs.getLong(1);
                        maxId = rs.getLong(2);
                    }
                }
            }

            if (maxId == 0) {
                System.out.println("⚠️ 表是空的或无法获取主键范围，无需同步。");
                return;
            }

            System.out.println("📊 ID 范围: " + minId + " ~ " + maxId);
            long totalRange = maxId - minId + 1;

            // 计算每个线程处理的步长 (Step)
            // 如果数据量很少，步长至少为 1
            long step = totalRange / THREAD_COUNT;
            if (step == 0) step = totalRange;

            // ------------------------------------------------
            // 第二阶段：切分任务并分发
            // ------------------------------------------------
            CountDownLatch latch = new CountDownLatch(THREAD_COUNT);
            AtomicLong totalRowsSynced = new AtomicLong(0);

            long currentStart = minId;

            for (int i = 0; i < THREAD_COUNT; i++) {
                long currentEnd = (i == THREAD_COUNT - 1) ? maxId : (currentStart + step - 1);

                // 提交任务
                executor.submit(new RangeSyncTask(
                        TABLE_NAME, PRIMARY_KEY, currentStart, currentEnd, latch, totalRowsSynced
                ));

                currentStart = currentEnd + 1;
                if (currentStart > maxId) break; // 防止溢出
            }

            // 等待所有线程完成
            System.out.println("⏳ " + THREAD_COUNT + " 个线程已启动，正在并行同步...");
            latch.await();

            long end = System.currentTimeMillis();
            System.out.println("✅ 同步完成! 总耗时: " + (end - start) / 1000 + " 秒");
            System.out.println("📦 总共处理行数: " + totalRowsSynced.get());

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            executor.shutdown();
        }
    }

    /**
     * 核心任务：同步指定 ID 范围的数据
     */
    static class RangeSyncTask implements Runnable {
        private String tableName;
        private String pkName;
        private long startId;
        private long endId;
        private CountDownLatch latch;
        private AtomicLong totalCounter;

        public RangeSyncTask(String tableName, String pkName, long startId, long endId, CountDownLatch latch, AtomicLong totalCounter) {
            this.tableName = tableName;
            this.pkName = pkName;
            this.startId = startId;
            this.endId = endId;
            this.latch = latch;
            this.totalCounter = totalCounter;
        }

        @Override
        public void run() {
            String threadName = Thread.currentThread().getName();
            // System.out.println("线程 " + threadName + " 处理范围: " + startId + " -> " + endId);

            try (Connection srcConn = DriverManager.getConnection(SRC_URL, SRC_USER, SRC_PASS);
                 Connection tgtConn = DriverManager.getConnection(TGT_URL, TGT_USER, TGT_PASS)) {

                // 1. 构建查询 SQL (带范围)
                String selectSql = "SELECT * FROM " + tableName + " WHERE " + pkName + " >= ? AND " + pkName + " <= ?";

                // 2. 获取元数据构建 Insert SQL
                PreparedStatement metaStmt = srcConn.prepareStatement(selectSql + " LIMIT 1");
                metaStmt.setLong(1, startId);
                metaStmt.setLong(2, startId); // 只是为了获取元数据，参数值不重要
                ResultSetMetaData metaData = metaStmt.getMetaData();
                int colCount = metaData.getColumnCount();
                metaStmt.close();

                StringBuilder insertSb = new StringBuilder();
                insertSb.append("INSERT IGNORE INTO ").append(tableName).append(" VALUES (");
                for (int i = 0; i < colCount; i++) insertSb.append(i==0?"?":",?");
                insertSb.append(")");

                // 3. 准备执行
                try (PreparedStatement srcPstmt = srcConn.prepareStatement(selectSql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
                     PreparedStatement tgtPstmt = tgtConn.prepareStatement(insertSb.toString())) {

                    // 开启流式读取 (防止该范围内数据依然很大导致 OOM)
                    srcPstmt.setFetchSize(Integer.MIN_VALUE);
                    srcPstmt.setLong(1, startId);
                    srcPstmt.setLong(2, endId);

                    ResultSet rs = srcPstmt.executeQuery();
                    int batchCount = 0;
                    int rowCount = 0;

                    while (rs.next()) {
                        for (int i = 1; i <= colCount; i++) {
                            tgtPstmt.setObject(i, rs.getObject(i));
                        }
                        tgtPstmt.addBatch();
                        batchCount++;
                        rowCount++;

                        if (batchCount >= BATCH_SIZE) {
                            tgtPstmt.executeBatch();
                            tgtPstmt.clearBatch();
                            batchCount = 0;
                        }
                    }
                    if (batchCount > 0) tgtPstmt.executeBatch();

                    totalCounter.addAndGet(rowCount);
                    System.out.println("   -> 线程 " + threadName + " 完成. 范围[" + startId + "-" + endId + "] 实际同步: " + rowCount + " 行");
                }

            } catch (Exception e) {
                System.err.println("❌ 线程 " + threadName + " 出错: " + e.getMessage());
                e.printStackTrace();
            } finally {
                latch.countDown();
            }
        }
    }

    // ================= 工具方法 =================

    private static boolean checkTableExists(Connection conn, String tableName) throws SQLException {
        try (ResultSet rs = conn.getMetaData().getTables(conn.getCatalog(), null, tableName, null)) {
            return rs.next();
        }
    }

    private static String getCreateTableSql(Connection conn, String tableName) throws SQLException {
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SHOW CREATE TABLE " + tableName)) {
            if (rs.next()) return rs.getString(2);
        }
        return "";
    }

    private static void executeSql(Connection conn, String sql) throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute(sql);
        }
    }
}