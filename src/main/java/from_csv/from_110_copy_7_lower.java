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
 */
public class from_110_copy_7_lower {
    public static void main(String[] args) {
        String sourceNode = "hadoop110";
        String[] targetNodes = {"hadoop104", "hadoop105", "hadoop106", "hadoop107", "hadoop108", "hadoop109", "hadoop110"};
        //String[] targetNodes = {"hadoop110"};
        String sourceTable = "dwd.ddddshoprelnewbeer";
        String targetTable = "test.ddddshoprelnewbeer_ss_dict";
        String joinColumn = "sellerid,shopname";  // 要和关联的表字段名一模一样，关联字段
        // 新增字段参数：需要转换为小写并替换中文括号的字段
        String lowercaseColumn = "shopname";  // 你需要转换的字段名称
        String clickHouseUrlTemplate = "jdbc:clickhouse://%s:8123?user=default&password=smartpath";
        List<String> columnNames = new ArrayList<>();
        List<String> columnTypes = new ArrayList<>();
        Map<String, String> columnRenameMap = new HashMap<>();  // 存储原字段名到新字段名的映射

        // 指定关联字段，可能需要和大表的字段一样，需要修改一下，不修改则赋予一样的值
        String originalColumnName = "";  // 原字段名
        String newColumnName = "";  // 新字段名

        // 如果需要更改字段名，添加到映射中
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
            }

            StringBuilder createTableQuery = new StringBuilder("CREATE TABLE " + targetTable + " (");
            for (int i = 0; i < columnNames.size(); i++) {
                createTableQuery.append(columnNames.get(i)).append(" ").append(columnTypes.get(i)).append(",");
            }
            createTableQuery.deleteCharAt(createTableQuery.length() - 1);
            createTableQuery.append(") ENGINE = Join(ANY, LEFT, ").append(joinColumn).append(")");
            System.out.println("Create table query: " + createTableQuery.toString());

            // Step 2: 从sourceNode读取表数据
            String selectDataQuery = "SELECT * FROM " + sourceTable;
            ResultSet dataResultSet = sourceStatement.executeQuery(selectDataQuery);
            List<List<String>> dataRows = new ArrayList<>();
            while (dataResultSet.next()) {
                List<String> row = new ArrayList<>();
                for (String columnName : columnNames) {
                    // 使用原始列名读取数据
                    String originalColumnNameForData = columnRenameMap.containsValue(columnName) ?
                            getKeyByValue(columnRenameMap, columnName) : columnName;
                    String columnValue = dataResultSet.getString(originalColumnNameForData);

                    // 如果是需要转换的字段，进行小写转换和括号转换
                    if (columnName.equals(lowercaseColumn)) {
                        columnValue = columnValue.toLowerCase();  // 转为小写
                        columnValue = columnValue.replace("（", "(").replace("）", ")");
                    }

                    row.add(columnValue);
                }
                dataRows.add(row);
            }

            // Step 3: 在每个targetNode上创建表并插入数据
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
