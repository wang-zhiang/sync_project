package from_csv.mysql;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class SqlServerToMySQLSync {

    // SQL Server 连接配置
    private static final String SQL_SERVER_URL = "jdbc:sqlserver://192.168.4.51;databaseName=pricetracking";
    private static final String SQL_SERVER_USER = "sa";
    private static final String SQL_SERVER_PASSWORD = "smartpathdata";

//    private static final String SQL_SERVER_URL = "jdbc:sqlserver://192.168.4.201:2422;databaseName=taobao_trading";
//    private static final String SQL_SERVER_USER = "CHH";
//    private static final String SQL_SERVER_PASSWORD = "Y1v606";
    // MySQL 连接配置
    private static final String MYSQL_URL = "jdbc:mysql://192.168.6.101:3306/saas_price_tracking";
    private static final String MYSQL_USER = "root";
    private static final String MYSQL_PASSWORD = "smartpath";

    // 批次大小
    private static final int BATCH_SIZE = 3000;

    public static void main(String[] args) {
        String tableName = "jd_item_info_20250410_20250410"; // 要同步的表名

        try {
            // 连接 SQL Server 和 MySQL
            Connection sqlServerConn = DriverManager.getConnection(SQL_SERVER_URL, SQL_SERVER_USER, SQL_SERVER_PASSWORD);
            Connection mysqlConn = DriverManager.getConnection(MYSQL_URL, MYSQL_USER, MYSQL_PASSWORD);

            // 自动建表
            createTableInMySQL(sqlServerConn, mysqlConn, tableName);

            // 批量导入数据
            syncData(sqlServerConn, mysqlConn, tableName);

            // 关闭连接
            sqlServerConn.close();
            mysqlConn.close();

            System.out.println("同步完成！");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 在 MySQL 中自动创建表
     */
    private static void createTableInMySQL(Connection sqlServerConn, Connection mysqlConn, String tableName) throws SQLException {
        // 获取 SQL Server 表结构
        DatabaseMetaData metaData = sqlServerConn.getMetaData();
        ResultSet columns = metaData.getColumns(null, null, tableName, null);

        // 构建建表 SQL
        StringBuilder createTableSql = new StringBuilder("CREATE TABLE " + tableName + " (");
        while (columns.next()) {
            String columnName = columns.getString("COLUMN_NAME");
            String columnType = columns.getString("TYPE_NAME");
            int columnSize = columns.getInt("COLUMN_SIZE");

            // 映射 SQL Server 数据类型到 MySQL 数据类型
            String mysqlType = mapDataTypeToMySQL(columnType, columnSize);
            createTableSql.append(columnName).append(" ").append(mysqlType).append(", ");
        }
        createTableSql.setLength(createTableSql.length() - 2); // 移除最后的逗号和空格
        createTableSql.append(")");

        // 在 MySQL 中执行建表 SQL
        try (Statement stmt = mysqlConn.createStatement()) {
            stmt.executeUpdate(createTableSql.toString());
            System.out.println("表创建成功: " + tableName);
        }
    }

    /**
     * 将 SQL Server 数据类型映射到 MySQL 数据类型
     */
    private static String mapDataTypeToMySQL(String sqlServerType, int size) {
        switch (sqlServerType.toUpperCase()) {
            case "NVARCHAR":
            case "VARCHAR":
                if (size > 16383) {
                    return "LONGTEXT"; // 超长字段映射为 LONGTEXT
                } else {
                    return "VARCHAR(" + size + ")"; // 普通长度字段映射为 VARCHAR
                }
            case "INT":
                return "INT";
            case "DATETIME":
                return "DATETIME";
            case "DECIMAL":
                return "DECIMAL(18, 2)";
            default:
                return "VARCHAR(255)"; // 默认类型
        }
    }

    /**
     * 同步数据
     */
    private static void syncData(Connection sqlServerConn, Connection mysqlConn, String tableName) throws SQLException {
        // 获取 SQL Server 表数据
        String selectSql = "SELECT * FROM " + tableName;
        try (Statement stmt = sqlServerConn.createStatement();
             ResultSet rs = stmt.executeQuery(selectSql)) {

            // 获取列名
            ResultSetMetaData metaData = rs.getMetaData();
            int columnCount = metaData.getColumnCount();
            List<String> columnNames = new ArrayList<>();
            for (int i = 1; i <= columnCount; i++) {
                columnNames.add(metaData.getColumnName(i));
            }

            // 构建插入 SQL
            StringBuilder insertSql = new StringBuilder("INSERT INTO " + tableName + " (");
            for (String columnName : columnNames) {
                insertSql.append(columnName).append(", ");
            }
            insertSql.setLength(insertSql.length() - 2); // 移除最后的逗号和空格
            insertSql.append(") VALUES (");
            for (int i = 0; i < columnCount; i++) {
                insertSql.append("?, ");
            }
            insertSql.setLength(insertSql.length() - 2); // 移除最后的逗号和空格
            insertSql.append(")");

            // 批量插入数据
            try (PreparedStatement pstmt = mysqlConn.prepareStatement(insertSql.toString())) {
                int count = 0;
                while (rs.next()) {
                    for (int i = 1; i <= columnCount; i++) {
                        pstmt.setObject(i, rs.getObject(i));
                    }
                    pstmt.addBatch();
                    count++;

                    // 每 BATCH_SIZE 条数据执行一次批次插入
                    if (count % BATCH_SIZE == 0) {
                        pstmt.executeBatch();
                        System.out.println("已插入 " + count + " 条数据");
                    }
                }
                // 插入剩余的数据
                pstmt.executeBatch();
                System.out.println("数据插入完成，总计插入 " + count + " 条数据");
            }
        }
    }
}