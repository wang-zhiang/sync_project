package mysqlutil;


import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class MySQLToSQLServerSync {

    // MySQL 源数据库配置
    private static final String SOURCE_HOST = "101.89.122.158";
    private static final String SOURCE_DATABASE = "eleme_app";
    private static final String SOURCE_TABLE = "pijiu_shop_eleme_new_address";
    private static final String SOURCE_USERNAME = "root";
    private static final String SOURCE_PASSWORD = "smartpath@123";
    private static final String MYSQL_JDBC_URL = "jdbc:mysql://" + SOURCE_HOST + "/" + SOURCE_DATABASE;

    // SQL Server 目标数据库配置
    private static final String TARGET_HOST = "192.168.4.39";
    private static final String TARGET_DATABASE = "o2o";
    private static final String TARGET_TABLE = "pijiu_shop_eleme_new_address_20250718_new";
    private static final String TARGET_USERNAME = "sa"; // 替换为实际用户名
    private static final String TARGET_PASSWORD = "smartpthdata"; // 替换为实际密码
    private static final String SQLSERVER_JDBC_URL = "jdbc:sqlserver://" + TARGET_HOST + ";databaseName=" + TARGET_DATABASE;

    // 同步配置
    private static final int BATCH_SIZE = 5000;
    private static final int THREAD_POOL_SIZE = 5;

    public static void main(String[] args) {
        try {
            // 自动创建目标表
            createTargetTableIfNotExists();

            // 获取源表总行数
            long totalRows = getSourceRowCount();
            System.out.println("Total rows to sync1: " + totalRows);

            // 计算每个线程处理的数据范围
            long rowsPerThread = totalRows / THREAD_POOL_SIZE;
            long remainder = totalRows % THREAD_POOL_SIZE;

            // 创建线程池
            ExecutorService executor = Executors.newFixedThreadPool(THREAD_POOL_SIZE);

            // 提交同步任务
            long startId = 0;
            for (int i = 0; i < THREAD_POOL_SIZE; i++) {
                long endId = startId + rowsPerThread - 1;
                if (i == THREAD_POOL_SIZE - 1) {
                    endId += remainder; // 最后一个线程处理剩余行
                }

                executor.submit(new SyncTask(startId, endId, i + 1));
                startId = endId + 1;
            }

            // 关闭线程池
            executor.shutdown();
            executor.awaitTermination(Long.MAX_VALUE, TimeUnit.MINUTES);

            System.out.println("Data synchronization completed!");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // 自动创建目标表
    private static void createTargetTableIfNotExists() throws SQLException {
        try (Connection srcConn = DriverManager.getConnection(MYSQL_JDBC_URL, SOURCE_USERNAME, SOURCE_PASSWORD);
             Connection destConn = DriverManager.getConnection(SQLSERVER_JDBC_URL, TARGET_USERNAME, TARGET_PASSWORD)) {

            // 获取MySQL表结构
            DatabaseMetaData metaData = srcConn.getMetaData();
            ResultSet columns = metaData.getColumns(null, null, SOURCE_TABLE, null);

            StringBuilder createTableSQL = new StringBuilder("CREATE TABLE ")
                    .append(TARGET_TABLE)
                    .append(" (");

            // 构建SQL Server建表语句
            List<String> columnDefs = new ArrayList<>();
            while (columns.next()) {
                String colName = columns.getString("COLUMN_NAME");
                String colType = columns.getString("TYPE_NAME");
                int colSize = columns.getInt("COLUMN_SIZE");

                // 类型映射（MySQL -> SQL Server）
                String sqlServerType;
                switch (colType.toUpperCase()) {
                    case "VARCHAR":
                    case "TEXT":
                        sqlServerType = "NVARCHAR(" + (colSize > 4000 ? "MAX" : colSize) + ")";
                        break;
                    case "INT":
                        sqlServerType = "INT";
                        break;
                    case "BIGINT":
                        sqlServerType = "BIGINT";
                        break;
                    case "DATETIME":
                    case "TIMESTAMP":
                        sqlServerType = "DATETIME2";
                        break;
                    case "DECIMAL":
                        sqlServerType = "DECIMAL(18, 6)";
                        break;
                    default:
                        sqlServerType = "NVARCHAR(MAX)";
                }

                columnDefs.add(colName + " " + sqlServerType);
            }

            createTableSQL.append(String.join(", ", columnDefs));
            createTableSQL.append(")");

            // 检查表是否存在
            if (!tableExists(destConn, TARGET_TABLE)) {
                try (Statement stmt = destConn.createStatement()) {
                    stmt.executeUpdate(createTableSQL.toString());
                    System.out.println("Table created: " + TARGET_TABLE);
                }
            }
        }
    }

    // 检查表是否存在
    private static boolean tableExists(Connection conn, String tableName) throws SQLException {
        DatabaseMetaData meta = conn.getMetaData();
        try (ResultSet rs = meta.getTables(null, null, tableName, null)) {
            return rs.next();
        }
    }

    // 获取源表总行数
    private static long getSourceRowCount() throws SQLException {
        try (Connection conn = DriverManager.getConnection(MYSQL_JDBC_URL, SOURCE_USERNAME, SOURCE_PASSWORD);
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM " + SOURCE_TABLE)) {
            if (rs.next()) {
                return rs.getLong(1);
            }
            return 0;
        }
    }

    // 数据同步任务
    static class SyncTask implements Runnable {
        private final long startId;
        private final long endId;
        private final int threadId;

        public SyncTask(long startId, long endId, int threadId) {
            this.startId = startId;
            this.endId = endId;
            this.threadId = threadId;
        }

        @Override
        public void run() {
            System.out.printf("Thread-%d started processing range: %d - %d%n", threadId, startId, endId);

            try (Connection srcConn = DriverManager.getConnection(MYSQL_JDBC_URL, SOURCE_USERNAME, SOURCE_PASSWORD);
                 Connection destConn = DriverManager.getConnection(SQLSERVER_JDBC_URL, TARGET_USERNAME, TARGET_PASSWORD)) {

                srcConn.setAutoCommit(false);
                destConn.setAutoCommit(false);

                long current = startId;
                while (current <= endId) {
                    // 从MySQL读取批次数据
                    String selectSQL = String.format(
                            "SELECT * FROM %s LIMIT %d OFFSET %d",
                            SOURCE_TABLE, BATCH_SIZE, current
                    );

                    try (Statement srcStmt = srcConn.createStatement();
                         ResultSet rs = srcStmt.executeQuery(selectSQL)) {

                        // 准备批量插入
                        DatabaseMetaData meta = srcConn.getMetaData();
                        ResultSetMetaData rsmd = rs.getMetaData();
                        int columnCount = rsmd.getColumnCount();

                        // 生成插入语句
                        StringBuilder insertSQL = new StringBuilder("INSERT INTO ")
                                .append(TARGET_TABLE)
                                .append(" VALUES (");
                        for (int i = 1; i <= columnCount; i++) {
                            insertSQL.append(i == columnCount ? "?)" : "?, ");
                        }

                        try (PreparedStatement pstmt = destConn.prepareStatement(insertSQL.toString())) {
                            int batchCount = 0;
                            while (rs.next()) {
                                for (int i = 1; i <= columnCount; i++) {
                                    pstmt.setObject(i, rs.getObject(i));
                                }
                                pstmt.addBatch();
                                batchCount++;

                                if (batchCount % BATCH_SIZE == 0) {
                                    pstmt.executeBatch();
                                    destConn.commit();
                                    batchCount = 0;
                                }
                                current++;
                            }
                            if (batchCount > 0) {
                                pstmt.executeBatch();
                                destConn.commit();
                            }
                        }
                        System.out.printf("Thread-%d processed %d records%n", threadId, current - startId);
                    }
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
            System.out.printf("Thread-%d completed! Processed %d records%n", threadId, endId - startId + 1);
        }
    }

    // 加载数据库驱动
    static {
        try {
            Class.forName("com.mysql.cj.jdbc.Driver");
            Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }
}