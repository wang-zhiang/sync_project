package mysqlutil;

import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import java.io.BufferedReader;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

public class get_sqlserver_schema {

    public static void main(String[] args) throws Exception {
        // 数据库连接信息
        String sqlServerUrl = "jdbc:sqlserver://192.168.3.183;DatabaseName=O2O;user=sa;password=smartpthdata";

        // 读取包含表名的CSV文件
        List<String> tableNames = readCsvFile("D:\\wzza\\develop\\idea_project\\ceshi\\src\\main\\java\\mysqlutil\\sqlserver_table.csv");

        // 创建Excel工作簿
        Workbook workbook = new XSSFWorkbook();
        Sheet sheet = workbook.createSheet("Query Results");

        // 写入标题行
        Row titleRow = sheet.createRow(0);
        titleRow.createCell(0).setCellValue("TableName");
        titleRow.createCell(1).setCellValue("Count(*)");
        titleRow.createCell(2).setCellValue("Sum(Price)");
        titleRow.createCell(3).setCellValue("Sum(MonthQty)");

        // 循环处理每个表名
        int rowId = 1;
        for (String tableName : tableNames) {
            QueryResult result = executeQuery(sqlServerUrl, tableName);

            // 写入Excel行
            Row row = sheet.createRow(rowId++);
            row.createCell(0).setCellValue(tableName);
            row.createCell(1).setCellValue(result.getCount());
            row.createCell(2).setCellValue(result.getSumPrice());
            row.createCell(3).setCellValue(result.getSumMonthQty());
        }

        // 将Excel写入文件
        try (FileOutputStream outputStream = new FileOutputStream("query_results.xlsx")) {
            workbook.write(outputStream);
        }
        workbook.close();
    }

    private static List<String> readCsvFile(String filePath) throws IOException {
        List<String> tableNames = new ArrayList<>();
        try (BufferedReader br = new BufferedReader(new FileReader(filePath))) {
            String line;
            while ((line = br.readLine()) != null) {
                tableNames.add(line.trim());
            }
        }
        return tableNames;
    }

    private static QueryResult executeQuery(String sqlServerUrl, String tableName) throws SQLException {
        String query = String.format("SELECT COUNT(*), SUM(Price), SUM(MonthQty) FROM %s", tableName);

        try (Connection connection = DriverManager.getConnection(sqlServerUrl);
             Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery(query)) {
            if (resultSet.next()) {
                return new QueryResult(resultSet.getInt(1), resultSet.getDouble(2), resultSet.getDouble(3));
            }
            return new QueryResult(0, 0, 0); // 如果没有结果，返回0
        }
    }

    static class QueryResult {
        private final int count;
        private final double sumPrice;
        private final double sumMonthQty;

        public QueryResult(int count, double sumPrice, double sumMonthQty) {
            this.count = count;
            this.sumPrice = sumPrice;
            this.sumMonthQty = sumMonthQty;
        }

        public int getCount() {
            return count;
        }

        public double getSumPrice() {
            return sumPrice;
        }

        public double getSumMonthQty() {
            return sumMonthQty;
        }
    }
}
