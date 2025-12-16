package from_csv;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;

/*
 * 从110上的某个表，复制七分字段表
 *
 * */
public class from_110_copy_7_usually {
    public static void main(String[] args) {
        String sourceNode = "192.168.5.110";
        String[] targetNodes = {"hadoop104", "hadoop105", "hadoop106", "hadoop107", "hadoop108", "hadoop109", "hadoop110"};
        //String[] targetNodes = {"192.168.5.111"};
        String sourceTable = "dwd.ddddshoprelnewbeer";
        String targetTable = "test.ddddshoprelnewbeer_ss_dict_assist";
        String clickHouseUrlTemplate = "jdbc:clickhouse://%s:8123?user=default&password=smartpath";
        List<String> columnNames = new ArrayList<>();
        List<String> columnTypes = new ArrayList<>();
        List<String> orderByColumns = new ArrayList<>();  // 存储ORDER BY字段
        Map<String, String> columnRenameMap = new HashMap<>();  // 存储原字段名到新字段名的映射

        // 如果需要更改字段名，添加到映射中
        String originalColumnName = "";  // 原字段名
        String newColumnName = "";  // 新字段名
        if (originalColumnName != null && newColumnName != null) {
            columnRenameMap.put(originalColumnName, newColumnName);
        }

        // Step 1: 从sourceNode读取表结构
        String sourceUrl = String.format(clickHouseUrlTemplate, sourceNode);
        try (Connection sourceConnection = DriverManager.getConnection(sourceUrl);
             Statement sourceStatement = sourceConnection.createStatement()) {

            String getColumnsQuery = "DESCRIBE TABLE " + sourceTable;
            ResultSet resultSet = sourceStatement.executeQuery(getColumnsQuery);

            while (resultSet.next()) {
                String columnName = resultSet.getString("name");
                String columnType = resultSet.getString("type");

                // 检查是否需要重命名字段
                if (columnRenameMap.containsKey(columnName)) {
                    columnName = columnRenameMap.get(columnName);
                }

                columnNames.add(columnName);
                columnTypes.add(columnType);

                // 假设 ORDER BY 字段是源表的某些列，您可以根据实际情况调整
                // 假设我们将第一个字段作为 ORDER BY 的依据，这里可以根据实际情况修改
                if (resultSet.getRow() == 1) {  // 默认选择第一个字段作为 ORDER BY
                    orderByColumns.add(columnName);
                }
            }

            // Step 2: 创建目标表的 CREATE TABLE 查询
            StringBuilder createTableQuery = new StringBuilder("CREATE TABLE " + targetTable + " (");
            for (int i = 0; i < columnNames.size(); i++) {
                createTableQuery.append(columnNames.get(i)).append(" ").append(columnTypes.get(i)).append(",");
            }
            createTableQuery.deleteCharAt(createTableQuery.length() - 1);

            // 使用 MergeTree 引擎，并指定 ORDER BY 字段
            createTableQuery.append(") ENGINE = MergeTree() ")
                    .append("ORDER BY ").append(orderByColumns.get(0)) // 选择一个字段作为 ORDER BY
                    .append(" SETTINGS index_granularity = 8192");
            System.out.println("Create table query: " + createTableQuery.toString());

            // Step 3: 从sourceNode读取表数据
            String selectDataQuery = "SELECT * FROM " + sourceTable;
            ResultSet dataResultSet = sourceStatement.executeQuery(selectDataQuery);
            List<List<String>> dataRows = new ArrayList<>();
            while (dataResultSet.next()) {
                List<String> row = new ArrayList<>();
                for (String columnName : columnNames) {
                    // 使用原始列名读取数据
                    String originalColumnNameForData = columnRenameMap.containsValue(columnName) ?
                            getKeyByValue(columnRenameMap, columnName) : columnName;
                    row.add(dataResultSet.getString(originalColumnNameForData));
                }
                dataRows.add(row);
            }

            // Step 4: 在每个targetNode上创建表并插入数据
            for (String node : targetNodes) {
                String targetUrl = String.format(clickHouseUrlTemplate, node);
                System.out.println("Connecting to node: " + node);

                try (Connection targetConnection = DriverManager.getConnection(targetUrl);
                     Statement targetStatement = targetConnection.createStatement()) {

                    // Drop table if it exists
                    String dropTableQuery = "DROP TABLE IF EXISTS " + targetTable;
                    targetStatement.execute(dropTableQuery);

                    // Create table
                    targetStatement.execute(createTableQuery.toString());
                    System.out.println("Table created successfully on node: " + node);

                    // Construct insert query
                    StringJoiner columnNamesJoiner = new StringJoiner(", ", "(", ")");
                    StringJoiner questionMarksJoiner = new StringJoiner(", ", "(", ")");
                    for (String column : columnNames) {
                        columnNamesJoiner.add(column);
                        questionMarksJoiner.add("?");
                    }

                    String insertSql = "INSERT INTO " + targetTable + " " + columnNamesJoiner + " VALUES " + questionMarksJoiner;

                    // Insert data into table
                    try (PreparedStatement preparedStatement = targetConnection.prepareStatement(insertSql)) {
                        for (List<String> row : dataRows) {
                            for (int i = 0; i < row.size(); i++) {
                                preparedStatement.setString(i + 1, row.get(i));
                            }
                            preparedStatement.addBatch();
                        }
                        preparedStatement.executeBatch();
                        System.out.println("Data inserted successfully on node: " + node);
                    }
                } catch (Exception e) {
                    System.err.println("Failed to process node: " + node);
                    e.printStackTrace();
                }
            }
        } catch (Exception e) {
            System.err.println("Failed to process source node: " + sourceNode);
            e.printStackTrace();
        }
    }

    // 根据值获取Map中的键
    private static String getKeyByValue(Map<String, String> map, String value) {
        for (Map.Entry<String, String> entry : map.entrySet()) {
            if (entry.getValue().equals(value)) {
                return entry.getKey();
            }
        }
        return null;
    }
}
