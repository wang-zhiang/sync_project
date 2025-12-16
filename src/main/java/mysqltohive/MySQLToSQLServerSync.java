package mysqltohive;

import java.sql.*;

public class MySQLToSQLServerSync {

    // MySQL 配置
    private static final String MYSQL_URL = "jdbc:mysql://101.89.122.158/jddj";
    private static final String MYSQL_USER = "root";
    private static final String MYSQL_PASSWORD = "smartpath@123";

    // SQL Server 配置
    private static final String SQLSERVER_URL = "jdbc:sqlserver://192.168.4.39;DatabaseName=trading_medicinenew;encrypt=true;trustServerCertificate=true;";
    private static final String SQLSERVER_USER = "sa";
    private static final String SQLSERVER_PASSWORD = "smartpthdata";

    // 批次大小
    private static final int BATCH_SIZE = 20000;

    // 要同步的表名
    private static final String SOURCE_TABLE_NAME = "goods_info_jddj_11"; // MySQL 表名
    private static final String TARGET_TABLE_NAME = "goods_info_jddj_11_new"; // SQL Server 目标表名

    public static void main(String[] args) {
        // 强制使用 TLSv1.2
//        System.setProperty("https.protocols", "TLSv1.2");
//        System.setProperty("jdk.tls.client.protocols", "TLSv1.2");

        Connection mysqlConn = null;
        Connection sqlServerConn = null;
        PreparedStatement insertStmt = null;
        Statement mysqlStmt = null;
        ResultSet rs = null;

        try {
            // 加载 JDBC 驱动
            Class.forName("com.mysql.cj.jdbc.Driver");
            Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");

            // 连接到 MySQL
            mysqlConn = DriverManager.getConnection(MYSQL_URL, MYSQL_USER, MYSQL_PASSWORD);
            mysqlStmt = mysqlConn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
            // 设置每次获取的行数，避免内存问题
            mysqlStmt.setFetchSize(Integer.MIN_VALUE);

            // 连接到 SQL Server
            sqlServerConn = DriverManager.getConnection(SQLSERVER_URL, SQLSERVER_USER, SQLSERVER_PASSWORD);
            sqlServerConn.setAutoCommit(false); // 手动提交事务

            // 检查目标表是否存在，如果不存在则创建
            if (!doesTableExist(sqlServerConn, TARGET_TABLE_NAME)) {
                createTargetTable(mysqlConn, sqlServerConn, SOURCE_TABLE_NAME, TARGET_TABLE_NAME);
                System.out.println("已创建目标表: " + TARGET_TABLE_NAME);
            } else {
                System.out.println("目标表已存在: " + TARGET_TABLE_NAME);
            }

            // 获取 MySQL 表的列信息
            DatabaseMetaData mysqlMeta = mysqlConn.getMetaData();
            ResultSet columns = mysqlMeta.getColumns(null, null, SOURCE_TABLE_NAME, null);
            StringBuilder columnNames = new StringBuilder();
            StringBuilder placeholders = new StringBuilder();
            int columnCount = 0;
            while (columns.next()) {
                String columnName = columns.getString("COLUMN_NAME");
                if (columnCount > 0) {
                    columnNames.append(", ");
                    placeholders.append(", ");
                }
                columnNames.append("[").append(columnName).append("]");
                placeholders.append("?");
                columnCount++;
            }
            columns.close();

            // 检查是否成功获取列信息
            if (columnCount == 0) {
                throw new SQLException("未找到表 " + SOURCE_TABLE_NAME + " 的列信息。");
            }

            // 构建插入 SQL
            String insertSQL = "INSERT INTO " + TARGET_TABLE_NAME + " (" + columnNames.toString() + ") VALUES (" + placeholders.toString() + ")";
            insertStmt = sqlServerConn.prepareStatement(insertSQL);

            // 查询 MySQL 数据
            String selectSQL = "SELECT * FROM " + SOURCE_TABLE_NAME;
            rs = mysqlStmt.executeQuery(selectSQL);

            int count = 0;
            int batchCount = 0;
            while (rs.next()) {
                for (int i = 1; i <= columnCount; i++) {
                    insertStmt.setObject(i, rs.getObject(i));
                }
                insertStmt.addBatch();
                count++;

                if (count % BATCH_SIZE == 0) {
                    insertStmt.executeBatch();
                    sqlServerConn.commit();
                    batchCount++;
                    System.out.println("已同步 " + (batchCount * BATCH_SIZE) + " 条记录...");
                }
            }

            // 执行剩余的批次
            if (count % BATCH_SIZE != 0) {
                insertStmt.executeBatch();
                sqlServerConn.commit();
                batchCount++;
                System.out.println("已同步 " + count + " 条记录...");
            }

            System.out.println("数据同步完成！总共同步了 " + count + " 条记录。");

        } catch (BatchUpdateException bue) {
            System.err.println("批量更新异常：");
            bue.printStackTrace();
            try {
                if (sqlServerConn != null) {
                    sqlServerConn.rollback();
                }
            } catch (SQLException se) {
                se.printStackTrace();
            }
        } catch (SQLException se) {
            System.err.println("SQL 异常：");
            se.printStackTrace();
            try {
                if (sqlServerConn != null) {
                    sqlServerConn.rollback();
                }
            } catch (SQLException se2) {
                se2.printStackTrace();
            }
        } catch (Exception e) {
            System.err.println("其他异常：");
            e.printStackTrace();
            try {
                if (sqlServerConn != null) {
                    sqlServerConn.rollback();
                }
            } catch (SQLException se) {
                se.printStackTrace();
            }
        } finally {
            // 关闭资源
            try { if (rs != null) rs.close(); } catch (SQLException e) { e.printStackTrace(); }
            try { if (mysqlStmt != null) mysqlStmt.close(); } catch (SQLException e) { e.printStackTrace(); }
            try { if (insertStmt != null) insertStmt.close(); } catch (SQLException e) { e.printStackTrace(); }
            try { if (mysqlConn != null) mysqlConn.close(); } catch (SQLException e) { e.printStackTrace(); }
            try { if (sqlServerConn != null) sqlServerConn.close(); } catch (SQLException e) { e.printStackTrace(); }
        }
    }

    /**
     * 检查目标表是否存在
     */
    private static boolean doesTableExist(Connection conn, String tableName) throws SQLException {
        DatabaseMetaData meta = conn.getMetaData();
        ResultSet rs = meta.getTables(null, null, tableName, new String[] {"TABLE"});
        boolean exists = rs.next();
        rs.close();
        return exists;
    }

    /**
     * 根据 MySQL 表结构在 SQL Server 中创建目标表
     */
    private static void createTargetTable(Connection mysqlConn, Connection sqlServerConn, String sourceTable, String targetTable) throws SQLException {
        // 获取 MySQL 表的列信息
        DatabaseMetaData mysqlMeta = mysqlConn.getMetaData();
        ResultSet columns = mysqlMeta.getColumns(null, null, sourceTable, null);
        StringBuilder createTableSQL = new StringBuilder("CREATE TABLE " + targetTable + " (");

        boolean first = true;
        while (columns.next()) {
            if (!first) {
                createTableSQL.append(", ");
            }
            first = false;

            String columnName = columns.getString("COLUMN_NAME");
            String dataType = columns.getString("TYPE_NAME");
            int columnSize = columns.getInt("COLUMN_SIZE");
            int nullable = columns.getInt("NULLABLE");

            String sqlServerType = mapMySQLTypeToSQLServer(dataType, columnSize);

            createTableSQL.append("[").append(columnName).append("] ").append(sqlServerType);
            if (nullable == DatabaseMetaData.columnNoNulls) {
                createTableSQL.append(" NOT NULL");
            } else {
                createTableSQL.append(" NULL");
            }
        }
        columns.close();
        createTableSQL.append(");");

        // 执行创建表的 SQL
        Statement stmt = sqlServerConn.createStatement();
        stmt.execute(createTableSQL.toString());
        stmt.close();
    }

    /**
     * 映射 MySQL 数据类型到 SQL Server 数据类型
     */
    private static String mapMySQLTypeToSQLServer(String mysqlType, int size) {
        mysqlType = mysqlType.toLowerCase();
        switch (mysqlType) {
            case "varchar":
            case "char":
                return "VARCHAR(" + size + ")";
            case "int":
            case "integer":
                return "INT";
            case "bigint":
                return "BIGINT";
            case "decimal":
            case "numeric":
                return "DECIMAL(18,2)";
            case "double":
                return "FLOAT";
            case "float":
                return "REAL";
            case "text":
                return "TEXT";
            case "date":
                return "DATE";
            case "datetime":
            case "timestamp":
                return "DATETIME";
            case "boolean":
                return "BIT";
            case "tinyint":
                return "TINYINT";
            case "smallint":
                return "SMALLINT";
            case "mediumint":
                return "INT";
            case "blob":
                return "VARBINARY(MAX)";
            default:
                // 默认使用 VARCHAR(MAX) 以防未映射的类型
                return "VARCHAR(MAX)";
        }
    }
}
