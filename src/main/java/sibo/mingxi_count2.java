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

public class  mingxi_count2{
/*
* 注意下面的时间范围,目前是（202301-202311）
*type只能填tm或者b2c ck表名依赖
* 一次性查两个数据库，a数据库不在会自动跳b数据库
* */
    public static void main(String[] args) throws Exception {
        // 数据库连接信息
        String type = "b2c"; // 或者 "b2c" //tm
        String sqlServerUrl1;
        String sqlServerUrl2;


        if (type.equals("tm")) {
            sqlServerUrl1 = "jdbc:sqlserver://192.168.4.201:2422;database=Taobao_trading";
            sqlServerUrl2 = "jdbc:sqlserver://smartnew.tpddns.cn:24222;database=Taobao_trading";
            //sqlServerUrl2 = "jdbc:sqlserver://192.168.4.35;database=Taobao_trading";
        } else if (type.equals("b2c")) {
            sqlServerUrl1 = "jdbc:sqlserver://192.168.4.212:2533;database=WebSearchC";
            sqlServerUrl2 = "jdbc:sqlserver://smartnew.tpddns.cn:25333;database=WebSearchC";
            //sqlServerUrl2 = "jdbc:sqlserver://192.168.4.36;database=WebSearchC";
        } else {
            throw new IllegalArgumentException("Invalid type: " + type);
        }
        String clickhouseUrl = "jdbc:clickhouse://hadoop110:8123";
        String sqlusername = "CHH";
        String sqlpassword = "Y1v606";
        String ckusername = "default";
        String ckpassword = "smartpath";

        // 读取 CSV 文件
        List<String> tableNames = readCsvFile("D:\\wzza\\develop\\idea_project\\ceshi\\src\\main\\java\\sibo\\a.csv");

        // 创建 Excel 工作簿
        Workbook workbook = new XSSFWorkbook();
        Sheet sheet = workbook.createSheet("Data Comparison");

        // 写入标题行
        Row titleRow = sheet.createRow(0);
        titleRow.createCell(0).setCellValue("ID");
        titleRow.createCell(1).setCellValue("SQL Server Table");
        titleRow.createCell(2).setCellValue("SQL Server Count(202401-202405)");
        titleRow.createCell(3).setCellValue("ClickHouse Count(202401-202405)");
        titleRow.createCell(4).setCellValue("Is Equal");

        // 循环处理每个表
        int rowId = 1;
        for (String tableName : tableNames) {
            long sqlServerCount = getCountFromSqlServer(sqlServerUrl1, sqlServerUrl2, sqlusername, sqlpassword, tableName);
            long clickhouseCount = getCountFromClickHouse(clickhouseUrl, ckusername, ckpassword, tableName,type);

            // 写入 Excel 行
            Row row = sheet.createRow(rowId++);
            row.createCell(0).setCellValue(rowId - 1);
            row.createCell(1).setCellValue(tableName);
            row.createCell(2).setCellValue(sqlServerCount);
            row.createCell(3).setCellValue(clickhouseCount);
            row.createCell(4).setCellValue(sqlServerCount == clickhouseCount);
        }

        // 将 Excel 写入文件
        try (FileOutputStream outputStream = new FileOutputStream("mingxi.xlsx")) {
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

    private static long getCountFromSqlServer(String sqlServerUrl1, String sqlServerUrl2, String sqlusername, String sqlpassword, String tableName) throws SQLException {
        long count = tryExecuteSql(sqlServerUrl1, sqlusername, sqlpassword, tableName);
        if (count == -1) {
            count = tryExecuteSql(sqlServerUrl2, sqlusername, sqlpassword, tableName);
        }
        return count;
    }

    private static long tryExecuteSql(String sqlServerUrl, String sqlusername, String sqlpassword, String tableName) {
        String tablename1 =tableName + "ty";
        try (Connection connection = DriverManager.getConnection(sqlServerUrl, sqlusername, sqlpassword);
             Statement statement = connection.createStatement()) {
            String query = "SELECT COUNT(*) FROM " + tablename1 + " where LEFT(CONVERT(varchar(100), AddDate, 20),7) >= '2024-01' and  LEFT(CONVERT(varchar(100), AddDate, 20),7) <= '2024-05'";
            System.out.println(query);
            ResultSet resultSet = statement.executeQuery(query);
            if (resultSet.next()) {

                return resultSet.getLong(1);
            }
            return 0;
        } catch (SQLException e) {
            System.out.println("Failed to execute on " + sqlServerUrl + " for table " + tablename1 + ": " + e.getMessage());
            return -1;
        }
    }

    private static long getCountFromClickHouse(String clickhouseUrl, String ckusername, String ckpassword, String tableName,String type) throws SQLException {
        try (Connection connection = DriverManager.getConnection(clickhouseUrl, ckusername, ckpassword);
             Statement statement = connection.createStatement()) {
            String tableName1 = tableName.toLowerCase();
            String query = "SELECT COUNT(*) FROM dwd.new_ec_"+ type +"_his_" + tableName1 + " WHERE  pt_ym  >= '202401' and  pt_ym  <= '202405'";
            ResultSet resultSet = statement.executeQuery(query);
            if (resultSet.next()) {
                return resultSet.getLong(1);
            }
            return 0;
        }
    }
}
