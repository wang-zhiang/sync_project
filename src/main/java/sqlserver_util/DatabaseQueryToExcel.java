package sqlserver_util;


import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.poi.ss.usermodel.*;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import java.io.*;
import java.sql.*;

public class DatabaseQueryToExcel {
    public static void main(String[] args) {
        String csvFilePath = "D:\\wzza\\develop\\idea_project\\ceshi\\src\\main\\java\\sqlserver_util\\a.csv"; // CSV文件路径
        String excelFilePath = "sqlserver_count.xlsx"; // 输出的Excel文件路径
        //String jdbcUrl = "jdbc:sqlserver://192.168.4.201:2422;DatabaseName=taobao_trading"; // 数据库URL
//        String jdbcUrl = "jdbc:sqlserver://192.168.3.182:1433;DatabaseName=SyncWebSearchJD"; // 数据库URL
        String jdbcUrl = "jdbc:sqlserver://192.168.4.36;DatabaseName=websearchc";
        String username = "CHH"; // 数据库用户名
        String password = "Y1v606"; // 数据库密码
//        String username = "sa"; // 数据库用户名
//        String password = "smartpthdata"; // 数据库密码
        //pdd
       // String jdbcUrl = "jdbc:sqlserver://smartpath10.tpddns.cn:2988;DatabaseName=WebSearchPinduoduo"; // 数据库URL
//        String username = "ldd"; // 数据库用户名
//        String password = "W1t459"; // 数据库密码


        try (Connection connection = DriverManager.getConnection(jdbcUrl, username, password)) {
            Workbook workbook = new XSSFWorkbook();
            Sheet sheet = workbook.createSheet("Results");

            int rowNum = 0;
            Row headerRow = sheet.createRow(rowNum++);
            headerRow.createCell(0).setCellValue("Table Name");
            headerRow.createCell(1).setCellValue("Count");
            headerRow.createCell(2).setCellValue("Sum(Price)");

            Reader in = new FileReader(csvFilePath);
            CSVParser parser = CSVFormat.DEFAULT.parse(in);  // 不再使用 withFirstRecordAsHeader()
            for (CSVRecord record : parser) {
                String tableName = record.get(0);  // 直接使用索引0访问第一列
                if (!tableName.isEmpty()) {
                    // 执行SQL查询
                    PreparedStatement stmt = connection.prepareStatement("SELECT COUNT(*) AS [RowCount], SUM(price) AS [PriceSum] FROM " + tableName);

                    ResultSet rs = stmt.executeQuery();
                    if (rs.next()) {
                        int count = rs.getInt("RowCount");
                        double sumPrice = rs.getDouble("PriceSum");

                        // 写入Excel文件
                        Row row = sheet.createRow(rowNum++);
                        row.createCell(0).setCellValue(tableName);
                        row.createCell(1).setCellValue(count);
                        row.createCell(2).setCellValue(sumPrice);
                    }
                    rs.close();
                    stmt.close();
                }
            }
            parser.close();

            // 将数据写入文件
            FileOutputStream out = new FileOutputStream(new File(excelFilePath));
            workbook.write(out);
            out.close();
            workbook.close();

            System.out.println("数据已成功写入Excel文件.");
        } catch (SQLException | IOException e) {
            e.printStackTrace();
        }
    }
}
