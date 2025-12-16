package sibo;

import org.apache.poi.ss.usermodel.*;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import java.io.*;
import java.sql.*;
import java.util.*;
//测试o2o数据量是否一致问题
public class o2o_count {

    public static void main(String[] args) throws Exception {
        // 数据库连接信息
        String sqlServerUrl = "jdbc:sqlserver://192.168.4.39;database=trading_medicinenew";
        String clickhouseUrl = "jdbc:clickhouse://hadoop110:8123";
        String sqlusername = "sa";
        String sqlpassword = "smartpthdata";
        String ckusername = "default";
        String ckpassword = "smartpath";



        // 连接数据库
        Connection sqlServerConnection = DriverManager.getConnection(sqlServerUrl, sqlusername, sqlpassword);
        Connection clickhouseConnection = DriverManager.getConnection(clickhouseUrl, ckusername, ckpassword);

        // 读取 CSV 文件
        List<String> tableNames = readCsvFile("D:\\wzza\\develop\\idea_project\\ceshi\\src\\main\\java\\sibo\\a.xlsx");

        // 创建 Excel 工作簿
        Workbook workbook = new XSSFWorkbook();
        Sheet sheet = workbook.createSheet("Data Comparison");

        // 写入标题行
        Row titleRow = sheet.createRow(0);
        titleRow.createCell(0).setCellValue("ID");
        titleRow.createCell(1).setCellValue("SQL Server Table");
        titleRow.createCell(2).setCellValue("SQL Server Count");
        titleRow.createCell(3).setCellValue("ClickHouse Count");
        titleRow.createCell(4).setCellValue("Is Equal");

        // 循环处理每个表
        int rowId = 1;
        for (String tableName : tableNames) {
            long sqlServerCount = getCountFromSqlServer(sqlServerConnection, tableName);
            long clickhouseCount = getCountFromClickHouse(clickhouseConnection, tableName);

            // 写入 Excel 行
            Row row = sheet.createRow(rowId++);
            row.createCell(0).setCellValue(rowId - 1);
            row.createCell(1).setCellValue(tableName);
            row.createCell(2).setCellValue(sqlServerCount);
            row.createCell(3).setCellValue(clickhouseCount);
            row.createCell(4).setCellValue(sqlServerCount == clickhouseCount);
        }

        // 关闭数据库连接
        sqlServerConnection.close();
        clickhouseConnection.close();

        // 将 Excel 写入文件
        try (FileOutputStream outputStream = new FileOutputStream("jiaoshu.xlsx")) {
            workbook.write(outputStream);
        }
        workbook.close();
    }

    private static List<String> readCsvFile(String filePath) throws IOException {
        List<String> tableNames = new ArrayList<>();
        try (BufferedReader br = new BufferedReader(new FileReader(filePath))) {
            String line;
            while ((line = br.readLine()) != null) {
                if (!line.trim().isEmpty()) {
                    tableNames.add(line);
                }
            }
        }
        return tableNames;
    }

    private static long getCountFromSqlServer(Connection connection, String tableName) throws SQLException {
        System.out.println(tableName);
        String query = "SELECT COUNT(*) FROM " + tableName;
        System.out.println(query);
        try (Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery(query)) {
            if (resultSet.next()) {
                return resultSet.getLong(1);
            }
            return 0;
        }
    }

    private static long getCountFromClickHouse(Connection connection, String tableName) throws SQLException {
        //String query = "SELECT COUNT(*) FROM dwd.O2O WHERE ImportFromTB = '" + tableName + "'";
        String query = "SELECT COUNT(*) FROM dwd.O2O WHERE ImportFromTB = '" + tableName + "' and ExtInt1 = 1" ;

        try (Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery(query)) {
            if (resultSet.next()) {
                return resultSet.getLong(1);
            }
            return 0;
        }
    }
}
