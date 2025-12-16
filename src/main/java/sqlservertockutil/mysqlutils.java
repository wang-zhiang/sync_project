package sqlservertockutil;

import scala.Int;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class mysqlutils {

    private static final String DB_HOST = "192.168.6.101"; // 数据库主机地址
    private static final int DB_PORT = 3306; // 数据库端口号
    private static final String DB_NAME = "etl"; // 数据库名称
    private static final String DB_USER = "root"; // 数据库用户名
    private static final String DB_PASSWORD = "smartpath"; // 数据库密码
    private static final String DB_DRIVER = "com.mysql.cj.jdbc.Driver";


    /**
     * 更新MySQL表中的数据
     *
     * @param sql    SQL语句
     * @param params 参数列表
     * @return 更新的行数
     */
    public static int update(String sql, Object... params) throws SQLException {
        Connection conn = null;
        PreparedStatement pstmt = null;
        try {
            // 加载MySQL数据库驱动程序
            Class.forName("com.mysql.cj.jdbc.Driver");
            // 建立数据库连接
            conn = DriverManager.getConnection(
                    "jdbc:mysql://" + DB_HOST + ":" + DB_PORT + "/" + DB_NAME +
                            "?useUnicode=true&characterEncoding=utf8&serverTimezone=Asia/Shanghai",
                    DB_USER, DB_PASSWORD
            );
            // 创建PreparedStatement对象，用于执行SQL语句
            pstmt = conn.prepareStatement(sql);
            // 为PreparedStatement对象设置参数
            for (int i = 0; i < params.length; i++) {
                pstmt.setObject(i + 1, params[i]);
            }
            // 执行SQL语句并返回更新的行数
            return pstmt.executeUpdate();
        } catch (ClassNotFoundException e) {
            throw new SQLException("Could not load JDBC driver class", e);
        } finally {
            // 关闭PreparedStatement对象
            if (pstmt != null) {
                pstmt.close();
            }
            // 关闭数据库连接
            if (conn != null) {
                conn.close();
            }
        }
    }

    // 插入数据的方法
    public static void insertDataIntoTable(String Sqltable,String sqlcount, String Cktable,String ckcount) {

        Connection conn = null;
        PreparedStatement stmt = null;

        try {
            // 1. 注册驱动
            Class.forName(DB_DRIVER);

            // 2. 获取连接对象
            conn = DriverManager.getConnection(
                    "jdbc:mysql://" + DB_HOST + ":" + DB_PORT + "/" + DB_NAME +
                            "?useUnicode=true&characterEncoding=utf8&serverTimezone=Asia/Shanghai",
                    DB_USER, DB_PASSWORD
            );

            // 3. 定义SQL语句
            String sql = "INSERT INTO etl_ceshi(Sqltable,sqlcount,Cktable,ckcount) VALUES (?, ?,?,?)";

            // 4. 创建PreparedStatement对象
            stmt = conn.prepareStatement(sql);
            stmt.setString(1, Sqltable);
            stmt.setString(2, sqlcount);
            stmt.setString(3, Cktable);
            stmt.setString(4, ckcount);

            // 5. 执行插入操作
            int rows = stmt.executeUpdate();
            System.out.println(rows + " rows inserted.");

        } catch (ClassNotFoundException | SQLException e) {
            e.printStackTrace();
        } finally {
            // 6. 关闭Statement和Connection对象
            try {
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






    public static void main(String[] args) throws SQLException {
        //update("UPDATE etl_ceshi SET appState = 'running' WHERE appId = 'application_1685947537959_0217';");
        insertDataIntoTable("afd","2","running","3");
}



}
