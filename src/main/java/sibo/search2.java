package sibo;

import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import java.io.BufferedReader;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

//测试o2o数据量是否一致问题
public class search2 {

    public static void main(String[] args) throws Exception {
        List<String> tableNames = readCsvFile("D:\\wzza\\develop\\idea_project\\ceshi\\src\\main\\java\\sibo\\a.xlsx");

        // 数据库连接信息
        String url = "jdbc:sqlserver://192.168.4.39;database=trading_medicine";
        String username = "sa";
        String password = "smartpthdata";

        try (Connection connection = DriverManager.getConnection(url, username, password);
             Workbook workbook = new XSSFWorkbook()) {
            Sheet sheet = workbook.createSheet("Query Results");
            boolean isHeaderWritten = false;

            for (String tableName : tableNames) {
                String[] split = tableName.split("&&&&");
                String tableName1 =split[0];
                int industyid = Integer.parseInt(split[1]);
                System.out.println("表名是："+ tableName1 + "industryid:" + industyid);

                String sql = buildSqlQuery(tableName1,industyid);
                try (PreparedStatement statement = connection.prepareStatement(sql);
                     ResultSet resultSet = statement.executeQuery()) {
                    writeResultSetToExcel(workbook, sheet, resultSet, isHeaderWritten);
                    if (!isHeaderWritten) isHeaderWritten = true;
                }
            }

            try (FileOutputStream outputStream = new FileOutputStream("query_results.xlsx")) {
                workbook.write(outputStream);
            }
        }
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

    private static String buildSqlQuery(String tableName,int industryid) {
        // 替换为你的实际 SQL 查询
        String sql  = "SELECT \n" +
                " '明细' type,t2.id AS industryid,t2.IndustryName as IndustryName,t3.id AS industrysubid,t3.Name as industrysubname,\n" +
                "\tsellerid,CAST(year(adddate)AS nvarchar(50))+RIGHT('00'+CAST(month(adddate) AS nvarchar(50)),2) ym,\n" +
                "  count(*) cnt,count(distinct(shop)) as shopcnt,\n" +
                "  sum(price * qty) as val,sum(qty) as vol,sum(price * qty)/1000 as val000,sum(qty)/1000 as vol000\n" +
                "FROM  " + tableName +" t1,industry t2,industrysub t3,category t4 \n" +
                "WHERE  t1.categoryid = t4.id AND t4.industryid = " + industryid + " AND t4.industryid = t2.id AND t4.spid = t3.id AND t4.industryid = t3.pid \n" +
                "group by t2.id,t2.IndustryName,t3.id,t3.Name,sellerid,\n" +
                "CAST(year(adddate)AS nvarchar(50))+RIGHT('00'+CAST(month(adddate) AS nvarchar(50)),2)";
        System.out.println("现在跑到：" + tableName);
        System.out.println(sql);
        return sql;
    }

    private static void writeResultSetToExcel(Workbook workbook, Sheet sheet, ResultSet resultSet, boolean isHeaderWritten) throws SQLException {
        ResultSetMetaData metaData = resultSet.getMetaData();
        int columnCount = metaData.getColumnCount();

        if (!isHeaderWritten) {
            Row headerRow = sheet.createRow(0);
            for (int i = 1; i <= columnCount; i++) {
                headerRow.createCell(i - 1).setCellValue(metaData.getColumnName(i));
            }
        }

        int rowIndex = sheet.getLastRowNum() + 1;
        while (resultSet.next()) {
            Row row = sheet.createRow(rowIndex++);
            for (int i = 1; i <= columnCount; i++) {
                row.createCell(i - 1).setCellValue(resultSet.getString(i));
            }
        }
    }
}
