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
 * 从110上的某个表，复制七分字段表，并分批次插入，每批次50,000条记录
 *
 * */
public class from_110_copy_7_batch {
    public static void main(String[] args) {
        String sourceNode = "hadoop110";
        String[] targetNodes = {
                "hadoop104", "hadoop105", "hadoop106",
                "hadoop107", "hadoop108", "hadoop109", "hadoop110"
        };

        // String[] targetNodes = {"hadoop109"};
        String sourceTable = "dim.dim.brandgenxing";
        String targetTable = "dim.skuall";
        String joinColumn = "sellerid,shopname";  // 要和关联的表字段名一模一样，关联字段
        String clickHouseUrlTemplate = "jdbc:clickhouse://%s:8123?user=default&password=smartpath";
        List<String> columnNames = new ArrayList<>();
        List<String> columnTypes = new ArrayList<>();
        Map<String, String> columnRenameMap = new HashMap<>();  // 存储原字段名到新字段名的映射

        // 指定关联字段，可能需要和大表的字段一样，需要修改一下，不修改则赋予一样的值
        String originalColumnName = "";  // 原字段名
        String newColumnName = "";  // 新字段名

        // 如果需要更改字段名，添加到映射中
        if (originalColumnName != null && !originalColumnName.isEmpty()
                && newColumnName != null && !newColumnName.isEmpty()) {
            columnRenameMap.put(originalColumnName, newColumnName);
        }

        // 定义每批次插入的记录数
        final int BATCH_SIZE = 50000;

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

            // 构建CREATE TABLE语句
            StringBuilder createTableQuery = new StringBuilder("CREATE TABLE " + targetTable + " (");
            for (int i = 0; i < columnNames.size(); i++) {
                createTableQuery.append(columnNames.get(i)).append(" ").append(columnTypes.get(i)).append(",");
            }
            createTableQuery.deleteCharAt(createTableQuery.length() - 1); // 删除最后的逗号
            createTableQuery.append(") ENGINE = Join(ANY, LEFT, ").append(joinColumn).append(")");
            System.out.println("Create table query: " + createTableQuery.toString());

            // Step 2: 在每个targetNode上创建表
            // 准备每个目标节点的连接和PreparedStatement
            Map<String, Connection> targetConnections = new HashMap<>();
            Map<String, PreparedStatement> targetPreparedStatements = new HashMap<>();
            Map<String, Integer> targetBatchCounts = new HashMap<>();

            for (String node : targetNodes) {
                String targetUrl = String.format(clickHouseUrlTemplate, node);
                System.out.println("Connecting to node: " + node);

                try {
                    Connection targetConnection = DriverManager.getConnection(targetUrl);
                    Statement targetStatement = targetConnection.createStatement();

                    // Drop table if it exists
                    String dropTableQuery = "DROP TABLE IF EXISTS " + targetTable;
                    targetStatement.execute(dropTableQuery);

                    // Create table
                    targetStatement.execute(createTableQuery.toString());
                    System.out.println("Table created successfully on node: " + node);

                    // 构建INSERT语句
                    StringJoiner columnNamesJoiner = new StringJoiner(", ", "(", ")");
                    StringJoiner questionMarksJoiner = new StringJoiner(", ", "(", ")");
                    for (String column : columnNames) {
                        columnNamesJoiner.add(column);
                        questionMarksJoiner.add("?");
                    }

                    String insertSql = "INSERT INTO " + targetTable + " "
                            + columnNamesJoiner + " VALUES " + questionMarksJoiner;
                    System.out.println("Insert SQL for node " + node + ": " + insertSql);

                    PreparedStatement preparedStatement = targetConnection.prepareStatement(insertSql);
                    targetConnections.put(node, targetConnection);
                    targetPreparedStatements.put(node, preparedStatement);
                    targetBatchCounts.put(node, 0);

                } catch (Exception e) {
                    System.err.println("Failed to setup target node: " + node);
                    e.printStackTrace();
                }
            }

            // Step 3: 从sourceNode读取表数据，并分批次插入到targetNodes
            String selectDataQuery = "SELECT * FROM " + sourceTable;
            ResultSet dataResultSet = sourceStatement.executeQuery(selectDataQuery);

            while (dataResultSet.next()) {
                for (int i = 0; i < columnNames.size(); i++) {
                    // 不需要重命名，已经在列名读取时处理
                }

                // 对于每个目标节点，添加到其PreparedStatement的batch中
                for (String node : targetNodes) {
                    PreparedStatement preparedStatement = targetPreparedStatements.get(node);
                    if (preparedStatement == null) {
                        continue; // 如果某个节点连接失败，跳过
                    }

                    for (int i = 0; i < columnNames.size(); i++) {
                        String columnName = columnNames.get(i);
                        String value = dataResultSet.getString(columnName);
                        preparedStatement.setString(i + 1, value);
                    }
                    preparedStatement.addBatch();

                    // 更新批次计数
                    int currentCount = targetBatchCounts.get(node) + 1;
                    targetBatchCounts.put(node, currentCount);

                    // 如果达到BATCH_SIZE，执行批次
                    if (currentCount % BATCH_SIZE == 0) {
                        try {
                            preparedStatement.executeBatch();
                            System.out.println("Inserted " + currentCount + " records into node: " + node);
                            targetBatchCounts.put(node, 0); // 重置计数
                        } catch (Exception e) {
                            System.err.println("Failed to insert batch into node: " + node);
                            e.printStackTrace();
                        }
                    }
                }
            }

            // Step 4: 插入剩余的记录
            for (String node : targetNodes) {
                PreparedStatement preparedStatement = targetPreparedStatements.get(node);
                if (preparedStatement == null) {
                    continue; // 如果某个节点连接失败，跳过
                }

                try {
                    preparedStatement.executeBatch();
                    System.out.println("Inserted remaining records into node: " + node);
                } catch (Exception e) {
                    System.err.println("Failed to insert remaining batch into node: " + node);
                    e.printStackTrace();
                }
            }

            // 关闭所有目标节点的连接和PreparedStatement
            for (String node : targetNodes) {
                try {
                    PreparedStatement preparedStatement = targetPreparedStatements.get(node);
                    if (preparedStatement != null) {
                        preparedStatement.close();
                    }
                    Connection targetConnection = targetConnections.get(node);
                    if (targetConnection != null) {
                        targetConnection.close();
                    }
                    System.out.println("Closed connection for node: " + node);
                } catch (Exception e) {
                    System.err.println("Failed to close connection for node: " + node);
                    e.printStackTrace();
                }
            }

            System.out.println("All nodes have been processed successfully.");

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
