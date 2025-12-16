

package ck_hive.hive_metastore;

        import ru.yandex.clickhouse.BalancedClickhouseDataSource;
        import ru.yandex.clickhouse.settings.ClickHouseProperties;

        import java.io.BufferedReader;
        import java.io.FileReader;
        import java.sql.*;
        import java.util.ArrayList;
        import java.util.List;

public class cktohive_new {
    public static void main(String[] args) {
        String clickhouseUrl = "jdbc:clickhouse://hadoop110:8123";
        String clickhouseDatabase = "dwd";
        String clickhouseUser = "default";
        String clickhousePassword = "smartpath";

        String hiveUrl = "jdbc:hive2://hadoop104:10000";
        String hiveDatabase = "dwd";
        String hiveUser = "smartpath";
        String hivePassword = "cl@1109";

        String csvFilePath = "D:\\wzza\\develop\\idea_project\\ceshi\\src\\main\\java\\ck_hive\\hive_metastore\\a.csv";
        createHiveTablesFromCSV(csvFilePath, clickhouseUrl, clickhouseDatabase, clickhouseUser, clickhousePassword,
                hiveUrl, hiveDatabase, hiveUser, hivePassword);
    }

    public static void createHiveTablesFromCSV(String csvFilePath, String clickhouseUrl, String clickhouseDatabase,
                                               String clickhouseUser, String clickhousePassword,
                                               String hiveUrl, String hiveDatabase, String hiveUser,
                                               String hivePassword) {

        ClickHouseProperties properties = new ClickHouseProperties();
        properties.setUser(clickhouseUser);
        properties.setPassword(clickhousePassword);
        BalancedClickhouseDataSource clickhouseDataSource = new BalancedClickhouseDataSource(clickhouseUrl + "/" + clickhouseDatabase, properties);

        try (BufferedReader br = new BufferedReader(new FileReader(csvFilePath))) {
            String tableName;
            while ((tableName = br.readLine()) != null) {
                createHiveTable(clickhouseDataSource, tableName.trim(), hiveUrl, hiveDatabase, hiveUser, hivePassword);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void createHiveTable(BalancedClickhouseDataSource clickhouseDataSource, String clickhouseTable,
                                       String hiveUrl, String hiveDatabase, String hiveUser, String hivePassword) {
        try (Connection clickhouseConnection = clickhouseDataSource.getConnection();
             Statement clickhouseStatement = clickhouseConnection.createStatement();
             ResultSet resultSet = clickhouseStatement.executeQuery("SELECT * FROM " + clickhouseTable + " LIMIT 0")) { // 获取表结构

            ResultSetMetaData metaData = resultSet.getMetaData();
            List<String> columnNames = new ArrayList<>();
            List<String> columnTypes = new ArrayList<>();
            boolean hasPtYm = false; // Flag to check if pt_ym exists
            boolean hasPtChannel = false; // Flag to check if pt_channel exists

            for (int i = 1; i <= metaData.getColumnCount(); i++) {
                columnNames.add(metaData.getColumnName(i));
                columnTypes.add(metaData.getColumnTypeName(i));
                if ("pt_ym".equalsIgnoreCase(metaData.getColumnName(i))) {
                    hasPtYm = true; // Found pt_ym column
                }
                if ("pt_channel".equalsIgnoreCase(metaData.getColumnName(i))) {
                    hasPtChannel = true; // Found pt_channel column
                }
            }

            // Create Hive table
            try (Connection hiveConnection = DriverManager.getConnection(hiveUrl + "/" + hiveDatabase, hiveUser, hivePassword);
                 Statement hiveStatement = hiveConnection.createStatement()) {

                StringBuilder createTableSQL = new StringBuilder("CREATE TABLE IF NOT EXISTS " + hiveDatabase + "." + clickhouseTable + " (");
                for (int i = 0; i < columnNames.size(); i++) {
                    if (!"pt_ym".equalsIgnoreCase(columnNames.get(i)) && !"pt_channel".equalsIgnoreCase(columnNames.get(i))) { // Exclude pt_ym and pt_channel from the main columns
                        createTableSQL.append(columnNames.get(i)).append(" ").append(mapToHiveType(columnTypes.get(i))).append(", ");
                    }
                }

                // Add partition information
                if (hasPtYm || hasPtChannel) {
                    createTableSQL.setLength(createTableSQL.length() - 2); // Remove trailing comma and space
                    createTableSQL.append(") PARTITIONED BY (");
                    if (hasPtChannel) { // pt_channel 在前
                        createTableSQL.append("pt_channel STRING");
                    }
                    if (hasPtYm) {
                        if (hasPtChannel) createTableSQL.append(", ");
                        createTableSQL.append("pt_ym STRING");
                    }
                    createTableSQL.append(") ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' STORED AS TEXTFILE");
                } else {
                    createTableSQL.setLength(createTableSQL.length() - 2); // Remove trailing comma and space
                    createTableSQL.append(") ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' STORED AS TEXTFILE");
                }

                System.out.println(createTableSQL.toString());
                hiveStatement.execute(createTableSQL.toString());
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static String mapToHiveType(String clickhouseType) {
        // 简单的类型映射，可以根据需要扩展
        switch (clickhouseType.toLowerCase()) {
            case "int32":
                return "INT";
            case "int64":
                return "BIGINT";
            case "string":
            case "text":
                return "STRING";
            case "float32":
                return "FLOAT";
            case "float64":
                return "DOUBLE";
            case "date":
                return "DATE";
            case "datetime":
                return "TIMESTAMP";
            default:
                return "STRING"; // 默认类型
        }
    }
}
