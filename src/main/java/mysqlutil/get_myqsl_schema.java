package mysqlutil;


import java.sql.Connection;
        import java.sql.DatabaseMetaData;
        import java.sql.DriverManager;
        import java.sql.ResultSet;

public class get_myqsl_schema {
    public static void main(String[] args) {
        // 数据库连接配置，请替换为您自己的设置
        String databaseUrl = "jdbc:mysql://192.168.5.34:3306/elm20210518";
        String user = "root";
        String password = "smartpthdata";
        String tableName = "goods";

        try {
            // 加载并注册JDBC驱动程序
            Class.forName("com.mysql.jdbc.Driver");

            // 建立数据库连接
            Connection connection = DriverManager.getConnection(databaseUrl, user, password);
            if (connection != null) {
                System.out.println("Successfully connected to MySQL database.");

                // 获取并打印表的元数据信息
                printTableMetaData(connection, tableName);

                // 关闭数据库连接
                connection.close();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void printTableMetaData(Connection connection, String tableName) throws Exception {
        DatabaseMetaData metaData = connection.getMetaData();

        // 获取指定表的列信息
        ResultSet rsColumns = metaData.getColumns(null, "elm20210518", tableName, null);

        while (rsColumns.next()) {
            String columnName = rsColumns.getString("COLUMN_NAME");
            String columnType = rsColumns.getString("TYPE_NAME");
            int columnSize = rsColumns.getInt("COLUMN_SIZE");

            System.out.println("Column Name: " + columnName + ", Type: " + columnType + ", Size: " + columnSize);
        }
        rsColumns.close();
    }
}
