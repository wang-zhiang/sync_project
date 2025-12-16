package from_csv.from_ck_111;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import sqlservertockutil.chinesetopinyin;

import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.StringJoiner;

public class from_csv_7_mergetree {
    public static void main(String[] args) {
        String csvFile = "D:\\wzza\\develop\\idea_project\\ceshi\\src\\main\\java\\from_csv\\csv_file\\source4_ldl_itemid\\remove_product_pool_0326_dict.csv";
        //String[] nodes = {"hadoop104", "hadoop105", "hadoop106", "hadoop107", "hadoop108", "hadoop109", "hadoop110"};
        String[] nodes = {"192.168.5.111"};
        String tableName = "ods.remove_product_pool_0326_dict_assist";


        String orderByColumn = null; // 使用 CSV 文件中的第一个字段

        for (String node : nodes) {
            String url = "jdbc:clickhouse://" + node + ":8123?user=default&password=smartpath";
            System.out.println("Connecting to node: " + node);

            try (Reader in = new InputStreamReader(new FileInputStream(csvFile), StandardCharsets.UTF_8);
                 Connection connection = DriverManager.getConnection(url);
                 Statement statement = connection.createStatement()) {

                // Drop table if it exists
                String dropTableQuery = "DROP TABLE IF EXISTS " + tableName;
                statement.execute(dropTableQuery);

                // Parse CSV and construct create table query
                CSVParser parser = new CSVParser(in, CSVFormat.DEFAULT.withFirstRecordAsHeader().withIgnoreEmptyLines(true));
                StringBuilder createTableQuery = new StringBuilder("CREATE TABLE " + tableName + " (");

                List<String> validColumns = new ArrayList<>();
                List<String> columnsWithPinyin = new ArrayList<>();  // 存储转换为拼音的字段名
                boolean firstColumnProcessed = false;

                for (String column : parser.getHeaderNames()) {
                    if (!column.trim().isEmpty()) {
                        String column1 = chinesetopinyin.chineseToPinyin(column);
                        createTableQuery.append(column1).append(" String,");
                        validColumns.add(column);
                        columnsWithPinyin.add(column1);
                        // 设置第一个字段为排序字段
                        if (!firstColumnProcessed) {
                            orderByColumn = column1;
                            firstColumnProcessed = true;
                        }
                    }
                }

                createTableQuery.deleteCharAt(createTableQuery.length() - 1);
                createTableQuery.append(") ENGINE = MergeTree() ORDER BY " + orderByColumn);
                System.out.println("Create table query: " + createTableQuery.toString());

                // Create table
                statement.execute(createTableQuery.toString());
                System.out.println("Table created successfully on node: " + node);

                // Construct insert query
                StringJoiner columnNames = new StringJoiner(", ", "(", ")");
                StringJoiner questionMarks = new StringJoiner(", ", "(", ")");
                for (String column : columnsWithPinyin) {
                    columnNames.add(column);
                    questionMarks.add("?");
                }

                String insertSql = "INSERT INTO " + tableName + " " + columnNames + " VALUES " + questionMarks;

                // Insert data into table
                try (PreparedStatement preparedStatement = connection.prepareStatement(insertSql)) {
                    int count = 0;
                    for (CSVRecord record : parser) {
                        if (record.isConsistent()) { // Check if the record has the same number of values as the header
                            int columnIndex = 1;
                            for (String column : validColumns) {
                                String data = record.isSet(column) ? record.get(column).trim() : "";
                                preparedStatement.setString(columnIndex++, data);
                            }
                            preparedStatement.addBatch();
                            if (++count % 1000 == 0) {
                                preparedStatement.executeBatch();
                            }
                        }
                    }
                    preparedStatement.executeBatch();
                    System.out.println("Data inserted successfully on node: " + node);
                }
            } catch (Exception e) {
                System.err.println("Failed to process node: " + node);
                e.printStackTrace();
            }
        }
    }
}
