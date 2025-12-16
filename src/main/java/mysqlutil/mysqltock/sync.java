package mysqlutil.mysqltock;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class sync {
    private static final int BATCH_SIZE = 3000;  // 每个批次的大小
    private static final int THREAD_POOL_SIZE = 7;  // 线程池大小

    public static void main(String[] args) {
        String clickhouseUrl = "jdbc:clickhouse://hadoop110:8123";
        String mysqlUrl = "jdbc:mysql://192.168.6.101:3306/datasystem?useSSL=false";
        String clickhouseUser = "default";

        String clickhousePassword = "smartpath";
        String mysqlUser = "root";
        String mysqlPassword = "smartpath";
        String clickhouseTableName = "tmp.amplify_avene_massskincare_ES";  // ClickHouse 表名
        String mysqlTableName = "amplify_avene_massskincare_ES";           // 你指定的 MySQL 表名
        String whereCondition = " Y  = 2025";

        try (Connection clickhouseConn = DriverManager.getConnection(clickhouseUrl, clickhouseUser, clickhousePassword)) {
            // 获取 ClickHouse 表结构并创建 MySQL 表
            createMySQLTable(clickhouseConn, mysqlUrl, mysqlUser, mysqlPassword, clickhouseTableName, mysqlTableName);

            // 获取 ClickHouse 表数据
            String selectSQL = "SELECT * FROM " + clickhouseTableName + (whereCondition.isEmpty() ? "" : " WHERE " + whereCondition);
            Statement stmt = clickhouseConn.createStatement();
            ResultSet data = stmt.executeQuery(selectSQL);
            ResultSetMetaData metaData = data.getMetaData();

            // 将数据加载到内存中
            List<Object[]> dataList = new ArrayList<>();
            while (data.next()) {
                Object[] row = new Object[metaData.getColumnCount()];
                for (int i = 1; i <= metaData.getColumnCount(); i++) {
                    row[i - 1] = data.getObject(i);
                }
                dataList.add(row);
            }

            // 创建线程池
            ExecutorService executor = Executors.newFixedThreadPool(THREAD_POOL_SIZE);

            // 将数据分发给多个线程处理
            int totalRows = dataList.size();
            int rowsPerThread = totalRows / THREAD_POOL_SIZE;
            for (int i = 0; i < THREAD_POOL_SIZE; i++) {
                int start = i * rowsPerThread;
                int end = (i == THREAD_POOL_SIZE - 1) ? totalRows : start + rowsPerThread;
                List<Object[]> subList = dataList.subList(start, end);
                executor.submit(new DataInsertTask(mysqlUrl, mysqlUser, mysqlPassword, mysqlTableName, metaData, subList));
            }

            // 关闭线程池
            executor.shutdown();
            executor.awaitTermination(1, TimeUnit.HOURS);  // 等待所有任务完成

            System.out.println("数据同步完成: " + mysqlTableName);

        } catch (SQLException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    // 创建 MySQL 表
    private static void createMySQLTable(Connection clickhouseConn, String mysqlUrl, String mysqlUser, String mysqlPassword, String clickhouseTableName, String mysqlTableName) throws SQLException {
        Statement stmt = clickhouseConn.createStatement();
        ResultSet tableInfo = stmt.executeQuery("DESCRIBE TABLE " + clickhouseTableName);

        StringBuilder createTableSQL = new StringBuilder("CREATE TABLE IF NOT EXISTS `"
                + mysqlTableName + "` (");
        while (tableInfo.next()) {
            String columnName = tableInfo.getString(1);
            String columnType = tableInfo.getString(2);
            createTableSQL.append("`").append(columnName).append("` ").append(mapClickHouseTypeToMySQL(columnType)).append(", ");
        }
        createTableSQL.delete(createTableSQL.length() - 2, createTableSQL.length());
        createTableSQL.append(") ENGINE = InnoDB;");

        try (Connection mysqlConn = DriverManager.getConnection(mysqlUrl, mysqlUser, mysqlPassword);
             Statement mysqlStmt = mysqlConn.createStatement()) {
            mysqlStmt.executeUpdate(createTableSQL.toString());
            System.out.println("MySQL表创建成功: " + mysqlTableName);
        }
    }

    // 数据插入任务
    private static class DataInsertTask implements Runnable {
        private final String mysqlUrl;
        private final String mysqlUser;
        private final String mysqlPassword;
        private final String mysqlTableName;
        private final ResultSetMetaData metaData;
        private final List<Object[]> dataList;
    
        public DataInsertTask(String mysqlUrl, String mysqlUser, String mysqlPassword, String mysqlTableName, ResultSetMetaData metaData, List<Object[]> dataList) {
            this.mysqlUrl = mysqlUrl;
            this.mysqlUser = mysqlUser;
            this.mysqlPassword = mysqlPassword;
            this.mysqlTableName = mysqlTableName;
            this.metaData = metaData;
            this.dataList = dataList;
        }
    
        @Override
        public void run() {
            long startTime = System.currentTimeMillis();
            String threadName = Thread.currentThread().getName();
            
            System.out.println("[" + threadName + "] 开始处理数据，总计: " + dataList.size() + " 条记录");
            
            try (Connection mysqlConn = DriverManager.getConnection(mysqlUrl, mysqlUser, mysqlPassword);
                 PreparedStatement insertStmt = mysqlConn.prepareStatement(buildInsertSQL(mysqlTableName, metaData))) {
                
                // 设置自动提交为false，提高性能
                mysqlConn.setAutoCommit(false);
                
                int batchCount = 0;
                int totalInserted = 0;
                int batchNumber = 1;
                
                for (int rowIndex = 0; rowIndex < dataList.size(); rowIndex++) {
                    Object[] row = dataList.get(rowIndex);
                    
                    // 设置参数
                    for (int i = 1; i <= metaData.getColumnCount(); i++) {
                        insertStmt.setObject(i, row[i - 1]);
                    }
                    insertStmt.addBatch();
                    batchCount++;
                    
                    // 当达到批次大小时执行批处理
                    if (batchCount >= BATCH_SIZE) {
                        long batchStartTime = System.currentTimeMillis();
                        int[] results = insertStmt.executeBatch();
                        mysqlConn.commit();
                        long batchEndTime = System.currentTimeMillis();
                        
                        totalInserted += batchCount;
                        System.out.println(String.format(
                            "[%s] 批次 %d 完成: 插入 %d 条数据，耗时 %d ms，总进度: %d/%d (%.2f%%)",
                            threadName, batchNumber, batchCount, 
                            (batchEndTime - batchStartTime), totalInserted, dataList.size(),
                            (double) totalInserted / dataList.size() * 100
                        ));
                        
                        batchCount = 0;
                        batchNumber++;
                        insertStmt.clearBatch();
                    }
                }
                
                // 处理剩余的数据
                if (batchCount > 0) {
                    long batchStartTime = System.currentTimeMillis();
                    int[] results = insertStmt.executeBatch();
                    mysqlConn.commit();
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
                
            } catch (SQLException e) {
                System.err.println("[" + threadName + "] ❌ 插入失败: " + e.getMessage());
                e.printStackTrace();
            }
        }
    }

    // 构造 MySQL 插入语句
    private static String buildInsertSQL(String mysqlTableName, ResultSetMetaData metaData) throws SQLException {
        StringBuilder insertSQL = new StringBuilder("INSERT INTO `" + mysqlTableName + "` (");
        int columnCount = metaData.getColumnCount();

        for (int i = 1; i <= columnCount; i++) {
            insertSQL.append("`").append(metaData.getColumnName(i)).append("`, ");
        }
        insertSQL.delete(insertSQL.length() - 2, insertSQL.length());
        insertSQL.append(") VALUES (");

        for (int i = 1; i <= columnCount; i++) {
            insertSQL.append("?, ");
        }
        insertSQL.delete(insertSQL.length() - 2, insertSQL.length());
        insertSQL.append(")");

        return insertSQL.toString();
    }

    // 映射 ClickHouse 数据类型到 MySQL 数据类型
    private static String mapClickHouseTypeToMySQL(String clickhouseType) {
        switch (clickhouseType.toLowerCase()) {
            case "int32":
            case "int64":
                return "BIGINT";
            case "float32":
            case "float64":
                return "DOUBLE";
            case "string":
                return "TEXT";
            case "date":
                return "DATE";
            case "datetime":
                return "DATETIME";
            default:
                return "VARCHAR(255)";
        }
    }
}