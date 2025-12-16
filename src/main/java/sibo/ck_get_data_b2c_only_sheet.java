package sibo;

import org.apache.poi.ss.usermodel.*;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import java.io.*;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class ck_get_data_b2c_only_sheet {
    private static final String clickhouseUrl = "jdbc:clickhouse://hadoop110:8123/dwd";
    private static final String username = "default";
    private static final String password = "smartpath";

    public static void main(String[] args) throws Exception {
        List<String> tableNames = readCsvFile("D:\\wzza\\develop\\idea_project\\ceshi\\src\\main\\java\\sibo\\ci对数.csv");
        try (Workbook workbook = new XSSFWorkbook()) {
            Sheet sheet = workbook.createSheet("CombinedData"); // 创建一个Sheet用于所有表的数据
            int rowIndex = 0; // 跟踪当前行索引

            for (String tableName : tableNames) {
                rowIndex = exportTableToExcel(workbook, sheet, tableName, rowIndex); // 更新行索引
            }

            try (FileOutputStream outputStream = new FileOutputStream("output.xlsx")) {
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

    private static int exportTableToExcel(Workbook workbook, Sheet sheet, String tableName, int startRowIndex) throws SQLException {
        String sql = constructSqlQuery(tableName);

        try (Connection connection = DriverManager.getConnection(clickhouseUrl, username, password);
             Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery(sql)) {
            ResultSetMetaData metaData = resultSet.getMetaData();
            int columnCount = metaData.getColumnCount();

            // Create header row if it's the first table
            if (startRowIndex == 0) {
                Row headerRow = sheet.createRow(startRowIndex++);
                for (int i = 1; i <= columnCount; i++) {
                    headerRow.createCell(i - 1).setCellValue(metaData.getColumnLabel(i));
                }
            }

            // Fill data rows
            int rowIndex = startRowIndex;
            while (resultSet.next()) {
                Row row = sheet.createRow(rowIndex++);
                for (int i = 1; i <= columnCount; i++) {
                    row.createCell(i - 1).setCellValue(resultSet.getString(i));
                }
            }

            // Auto-size columns on the first write
            if (startRowIndex == 1) {
                for (int i = 0; i < columnCount; i++) {
                    sheet.autoSizeColumn(i);
                }
            }

            return rowIndex; // 返回更新后的行索引
        }
    }

    private static String constructSqlQuery(String tableName) {
        String[] split = tableName.split("&&&&");
        String  result = "select '明细',industryid,IndustryName,industrysubid,t5.Name as industrysubname,'B2C' as channel,\n" +
                "       formatDateTime(toDateTime(adddate) , '%Y%m') as ym,\n" +
                "       count(*) cnt,count(distinct lower(trim(shop))) as shopcnt,\n" +
                "       sum(price * qty) as val,sum(qty) as vol,\n" +
                "       sum(price * qty)/1000 as val000,\n" +
                "       sum(qty)/1000 as vol000,\n" +
                "       sum(zk_price * qty) as zkval,\n" +
                "       sum(zk_price * qty)/1000 as zkval000,\n" +
                "       sum(price * vol2) as preVal,(sum(vol2)) preVol,sum(zk_price * vol2) zkvalue from(\n" +
                "    select adddate,industryid,t2.IndustryName as IndustryName,t3.SPId as industrysubid,categoryid,sellerid,price,zk_price,qty,vol2,shop from (\n" +
                "        select adddate,price,qty,if(toFloat64OrNull(zk_price) is null,0,cast(zk_price as Float64)) zk_price,\n" +
                "        if(toFloat64OrNull(vol2) is null,0,cast(vol2 as Float64)) vol2,shop," + split[1] +" industryid,categoryid,sellerid\n" +
                "        from dwd." + split[0].toLowerCase() + " where adddate >= '2019-01-01' and adddate < '2024-06-01' and categoryid != 999999999\n" +
                "    ) t1\n" +
                "    left join dim.Industry t2 on t1.industryid = t2.Id\n" +
                "    left join dim.category t3 on t1.categoryid = t3.Id and t1.industryid = t3.IndustryId\n" +
                ") t4\n" +
                "left join dim.Industrysub t5 on t4.industrysubid = t5.Id and t5.Pid = t4.industryid\n" +
                "group by industryid,IndustryName,industrysubid,t5.Name,formatDateTime(toDateTime(adddate) , '%Y%m') order by formatDateTime(toDateTime(adddate) , '%Y%m') ;";
        System.out.println(result);
        return result;
    }
}
