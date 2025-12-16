package sqlservertockutil;

import java.sql.*;

public class sqlserverUtil {
        private static final String DRIVER_CLASS = "com.microsoft.sqlserver.jdbc.SQLServerDriver";



        public static void executeSqlCommand(String URL,String USERNAME,String  PASSWORD, String sql) {
            Connection conn = null;
            Statement stmt = null;
            ResultSet rs = null;

            try {
                // 加载SQL Server驱动类
                Class.forName(DRIVER_CLASS);

                // 连接数据库
                conn = DriverManager.getConnection(URL, USERNAME, PASSWORD);

                // 创建执行SQL的Statement对象
                stmt = conn.createStatement();

                // 执行SQL命令
                stmt.execute(sql);

                // 如果需要获取查询结果
                // rs = stmt.executeQuery(sql);

                // 处理查询结果（如果有查询结果）

            } catch (ClassNotFoundException e) {
                e.printStackTrace();
            } catch (SQLException e) {
                e.printStackTrace();
            } finally {
                // 关闭连接和释放资源
                try {
                    if (rs != null) {
                        rs.close();
                    }
                    if (stmt != null) {
                        stmt.close();
                    }
                    if (conn != null) {
                        conn.close();
                    }
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }

    public static ResultSet executeQuery(String url, String user, String password, String query) {
        Connection conn = null;
        Statement stmt = null;
        ResultSet rs = null;

        try {
            // 连接数据库
            conn = DriverManager.getConnection(url, user, password);

            // 创建 Statement 对象
            stmt = conn.createStatement();

            // 执行查询语句
            rs = stmt.executeQuery(query);

            // 返回结果集

        } catch(Exception e) {

            rs  = new myhResultset(); // 返回一个空的结果
           // e.printStackTrace();
        }


        return rs;
    }



        //输出true或者false
    public static boolean isTableExist( String URL, String USERNAME, String PASSWORD, String tableName) {
        Connection connection = null;
        Statement statement = null;
        ResultSet resultSet = null;
        boolean isExist = false;

        try {
            Class.forName(DRIVER_CLASS);
            connection = DriverManager.getConnection(URL, USERNAME, PASSWORD);

            // 创建 SQL 语句
            String sql = "SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '" + tableName + "'";

            // 创建并执行 SQL 查询
            statement = connection.createStatement();
            resultSet = statement.executeQuery(sql);

            // 判断结果集中是否有数据
            if (resultSet.next()) {
                isExist = true;
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } finally {
            // 关闭数据库连接
            try {
                if (resultSet != null) {
                    resultSet.close();
                }
                if (statement != null) {
                    statement.close();
                }
                if (connection != null) {
                    connection.close();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

        return isExist;
    }

        public static void main(String[] args) {
            // SQL Server数据库连接信息
            String URL = "jdbc:sqlserver://192.168.4.201:2422;DatabaseName=Taobao_trading";
            String USERNAME = "CHH";
            String PASSWORD = "Y1v606";
            String sql = "DELETE FROM 表名 WHERE 条件;";
            String tablename = "Allen测试同步ck的表";
            //executeSqlCommand(URL,USERNAME,PASSWORD,sql);
            System.out.println(isTableExist(URL, USERNAME, PASSWORD, tablename));
        }





}
