package ckutil;


import java.sql.*;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class kill_mudation {
    public static void main(String[] args) {
        String url = "jdbc:clickhouse://hadoop104:8123";
        String user = "default";
        String password = "smartpath";

        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

        boolean success = false;
        while (!success) {
            try (Connection connection = DriverManager.getConnection(url, user, password)) {
                DriverManager.setLoginTimeout(6000);
                Statement statement = connection.createStatement();

                // 查询最新失败时间
                String query = "SELECT DISTINCT create_time FROM system.mutations WHERE is_done = 0";
                ResultSet resultSet = statement.executeQuery(query);

                while (resultSet.next()) {
                    Timestamp latestFailTime = resultSet.getTimestamp("create_time");
                    LocalDateTime localDateTime = latestFailTime.toLocalDateTime();
                    String formattedDateTime = localDateTime.format(formatter);

                    // 执行KILL MUTATION命令
                    String killQuery = "KILL MUTATION WHERE database='dwd' AND table='O2O_test_local' AND create_time = '" + formattedDateTime + "'";
                    statement.executeQuery(killQuery);

                    System.out.println("Executed KILL MUTATION for latest_fail_time: " + formattedDateTime);
                }

                success = true; // 执行成功标志
            } catch (SQLException e) {
                e.printStackTrace();
                System.out.println("Query failed. Retrying...");
                // 捕获到异常后继续下一轮循环
            }
        }
    }
}
