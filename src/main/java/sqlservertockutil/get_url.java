package sqlservertockutil;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
public class get_url {





    public static String getValidSQLServerURL(String url1, String url2, String username, String password, String tableName) {
            Connection connection = null;

            // 尝试在第一个SQL Server URL上检查表是否存在
            try {
                String sqlServerUrl = url1;
                Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
                connection = DriverManager.getConnection(sqlServerUrl, username, password);
                ResultSet tablesResultSet = connection.getMetaData().getTables(null, null, tableName, null);
                if (tablesResultSet.next()) {
                    System.out.println("在第一个SQL Server URL上找到表：" + tableName);
                    return sqlServerUrl;
                }
            } catch (Exception e) {
                // 发生异常，继续尝试第二个URL
            } finally {
                closeConnection(connection);
            }

            // 尝试在第二个SQL Server URL上检查表是否存在
            try {
                String sqlServerUrl = url2;
                Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
                connection = DriverManager.getConnection(sqlServerUrl, username, password);
                ResultSet tablesResultSet = connection.getMetaData().getTables(null, null, tableName, null);
                if (tablesResultSet.next()) {
                    System.out.println("在第二个SQL Server URL上找到表：" + tableName);
                    return sqlServerUrl;
                }
            } catch (Exception e) {
                // 发生异常，两个URL都没有找到表
            } finally {
                closeConnection(connection);
            }

            // 如果两个URL都没有找到表，返回空字符串
            return "";
        }

        private static void closeConnection(Connection connection) {
            if (connection != null) {
                try {
                    connection.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }


