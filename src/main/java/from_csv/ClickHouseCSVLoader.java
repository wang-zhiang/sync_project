package from_csv;

import org.apache.commons.csv.*;
import sqlservertockutil.chinesetopinyin;

import java.io.FileReader;
import java.io.Reader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.util.StringJoiner;

//csv文件要先用记事本转一下，也可在代码里添加自动转
//

public class ClickHouseCSVLoader {
    public static void main(String[] args) {
        String csvFile = "D:\\wzza\\develop\\idea_project\\ceshi\\src\\main\\java\\from_csv\\leimu_2408.csv"; // CSV 文件路径
        String url = "jdbc:clickhouse://hadoop110:8123/test?user=default&password=smartpath"; // ClickHouse 数据库URL
        String tableName = "test.leimu_2408"; // 设置表名

        try (Reader in = new FileReader(csvFile);
             Connection connection = DriverManager.getConnection(url);
             Statement statement = connection.createStatement()) {

            // 先检查表是否存在，如果存在则删除
            String dropTableQuery = "DROP TABLE IF EXISTS " + tableName;
            statement.execute(dropTableQuery);

            CSVParser parser = new CSVParser(in, CSVFormat.DEFAULT.withFirstRecordAsHeader());
            StringBuilder createTableQuery = new StringBuilder("CREATE TABLE " + tableName + " (");

            String orderByColumn = null;
            boolean isFirst = true;

            for (String column : parser.getHeaderNames()) {
                String column1 = chinesetopinyin.chineseToPinyin(column);
                if (isFirst) {
                    orderByColumn = column1; // 第一列作为 ORDER BY 字段
                    isFirst = false;
                }
                createTableQuery.append(column1).append(" String,");
            }

            createTableQuery.deleteCharAt(createTableQuery.length() - 1); // 删除最后的逗号
            createTableQuery.append(") ENGINE = MergeTree() ORDER BY ").append(orderByColumn);
            System.out.println(createTableQuery.toString());
            // 执行创建表的 SQL 语句
            statement.execute(createTableQuery.toString());

            // 准备插入数据的 PreparedStatement
            StringJoiner columnNames = new StringJoiner(", ", "(", ")");
            StringJoiner questionMarks = new StringJoiner(", ", "(", ")");
            for (String column : parser.getHeaderNames()) {
                String column1 = chinesetopinyin.chineseToPinyin(column);
                columnNames.add(column1);
                questionMarks.add("?");
            }
            String insertSql = "INSERT INTO " + tableName + " " + columnNames + " VALUES " + questionMarks;
            PreparedStatement preparedStatement = connection.prepareStatement(insertSql);

            // 逐行读取 CSV 数据并插入到表中
            int count = 0;
            for (CSVRecord record : parser) {
                int columnIndex = 1;
                for (String column : parser.getHeaderNames()) {
                    preparedStatement.setString(columnIndex++, record.get(column));
                }
                preparedStatement.addBatch();
                if (++count % 1000 == 0) {
                    preparedStatement.executeBatch(); // 执行批量插入
                }
            }
            preparedStatement.executeBatch(); // 插入剩余的记录

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
