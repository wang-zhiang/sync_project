package sibo;

import org.apache.poi.ss.usermodel.*;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import java.io.*;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class ck_get_data_tm {
    private static final String clickhouseUrl = "jdbc:clickhouse://hadoop110:8123/dwd";
    private static final String username = "default";
    private static final String password = "smartpath";

    public static void main(String[] args) throws Exception {
        List<String> tableNames = readCsvFile("D:\\wzza\\develop\\idea_project\\ceshi\\src\\main\\java\\sibo\\a.xlsx");
        try (Workbook workbook = new XSSFWorkbook()) {
            for (String tableName : tableNames) {
                exportTableToExcel(workbook, tableName);
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

    private static void exportTableToExcel(Workbook workbook, String tableName) throws SQLException {
        Sheet sheet = workbook.createSheet(tableName);
        String sql = constructSqlQuery(tableName);

        try (Connection connection = DriverManager.getConnection(clickhouseUrl, username, password);
             Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery(sql)) {
            ResultSetMetaData metaData = resultSet.getMetaData();
            int columnCount = metaData.getColumnCount();

            // Create header row
            Row headerRow = sheet.createRow(0);
            for (int i = 1; i <= columnCount; i++) {
                headerRow.createCell(i - 1).setCellValue(metaData.getColumnLabel(i));
            }

            // Fill data rows
            int rowIndex = 1;
            while (resultSet.next()) {
                Row row = sheet.createRow(rowIndex++);
                for (int i = 1; i <= columnCount; i++) {
                    row.createCell(i - 1).setCellValue(resultSet.getString(i));
                }
            }

            // Auto-size columns
            for (int i = 0; i < columnCount; i++) {
                sheet.autoSizeColumn(i);
            }
        }
    }

    private static String constructSqlQuery(String tableName) {
        String[] split = tableName.split("&&&&");
        String  result = "select '明细',industryid,IndustryName,industrysubid,t5.Name as industrysubname,t4.channel channel,\n" +
                "       formatDateTime(toDateTime(adddate) , '%Y%m') as ym,\n" +
                "       count(*) cnt,count(distinct (sellerid)) as shopcnt,\n" +
                "       sum(price * qty * if(toFloat64OrNull(coef) is null,1,cast(coef as Float64))) as valcoef,\n" +
                "       sum(qty * if(toFloat64OrNull(coef) is null,1,cast(coef as Float64))) as volcoef,\n" +
                "       sum(price * qty * if(toFloat64OrNull(coef) is null,1,cast(coef as Float64)))/1000 as valcoef000,\n" +
                "       sum(qty * if(toFloat64OrNull(coef) is null,1,cast(coef as Float64)))/1000 as volcoef000,\n" +
                "       sum(zk_price * qty * if(toFloat64OrNull(coef) is null,1,cast(coef as Float64))) as zkvalcoef,\n" +
                "       sum(zk_price * qty * if(toFloat64OrNull(coef) is null,1,cast(coef as Float64)))/1000 as zkvalcoef000,\n" +
                "       sum(price * qty) as val,\n" +
                "       sum(qty) as vol,\n" +
                "       sum(zk_price * qty) as zkval from(\n" +
                "    select adddate,industryid,t2.IndustryName as IndustryName,t3.SPId as industrysubid,categoryid,sellerid,channel,price,zk_price,qty,grandInt,grand_new from (\n" +
                "        select adddate,price,qty,if(toFloat64OrNull(zk_price) is null,0,cast(zk_price as Decimal(9,2))) zk_price," + split[1] +" industryid,categoryid,sellerid,if(grand = 'false','淘宝',if(grand = 'true','天猫','')) channel,if(grand = 'false',0,if(grand = 'true',1,999999999)) grandInt,grand_new\n" +
                "        from dwd." + split[0].toLowerCase() + " where adddate >= '2021-03-01' and adddate < '2021-04-01' and categoryid != 999999999\n" +
                "    ) t1\n" +
                "    left join dim.Industry t2 on t1.industryid = t2.Id\n" +
                "    left join dim.category t3 on t1.categoryid = t3.Id and t1.industryid = t3.IndustryId\n" +
                ") t4\n" +
                "left join dim.Industrysub t5 on t4.industrysubid = t5.Id and t5.Pid = t4.industryid\n" +
                "left join dwd.tmCoefShopType t6 on t4.industrysubid = t6.spid and if(toInt32OrNull(t4.grand_new) is null,999999999,cast(t4.grand_new as Int32)) = t6.grandnew\n" +
                "and concat(toString(toYear(toDateTime(t4.adddate))),'-',toString(toMonth(toDateTime(t4.adddate)))) = t6.month_new\n" +
                "and t6.site in('c','m','q','h','s','w','z','a','l','e','k','n','t','y','aa')\n" +
                "group by industryid,IndustryName,industrysubid,t5.Name,formatDateTime(toDateTime(adddate) , '%Y%m'),t4.channel";
        return result;
    }
}
