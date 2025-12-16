package sqlservertockutil.不经过linux同步;

import au.com.bytecode.opencsv.CSVReader;
import org.apache.poi.ss.usermodel.*;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class for_count_sp {
    // SQL Server 连接信息
   // private static final String SQL_SERVER_URL = "jdbc:sqlserver://192.168.99.35:2766;databaseName=SearchCommentYHD";
    private static final String SQL_SERVER_URL = "jdbc:sqlserver://192.168.99.39:2800;databaseName=WebSearch";
    private static final String SQL_SERVER_USER = "sa";
    private static final String SQL_SERVER_PASSWORD = "smartpthdata";

    // ClickHouse 连接信息
    private static final String CLICKHOUSE_URL = "jdbc:clickhouse://hadoop110:8123";
    private static final String CLICKHOUSE_USER = "default";
    private static final String CLICKHOUSE_PASSWORD = "smartpath";
    private static final String CLICKHOUSE_TABLE_NAME = "dwd.spB2CWebsiteData";

    public static void main(String[] args) {
        try {
            // 读取 CSV 文件，获取表名列表
            List<String> tableNames = readCsvFile("D:\\wzza\\develop\\idea_project\\ceshi\\src\\main\\java\\sqlservertockutil\\不经过linux同步\\b.csv");

            // 创建Excel文件
            Workbook workbook = new XSSFWorkbook();
            Sheet sheet = workbook.createSheet("DataCounts");

            // 创建标题行
            Row headerRow = sheet.createRow(0);
            headerRow.createCell(0).setCellValue("SQL Server Table Name");
            headerRow.createCell(1).setCellValue("ClickHouse Table Name");
            headerRow.createCell(2).setCellValue("SQL Server Row Count");
            headerRow.createCell(3).setCellValue("ClickHouse Row Count");
            headerRow.createCell(4).setCellValue("Equal");

            // 获取 SQL Server 和 ClickHouse 的连接
            Connection sqlServerConn = DriverManager.getConnection(SQL_SERVER_URL, SQL_SERVER_USER, SQL_SERVER_PASSWORD);
            Connection clickHouseConn = DriverManager.getConnection(CLICKHOUSE_URL, CLICKHOUSE_USER, CLICKHOUSE_PASSWORD);

            int rowNum = 1;

            for (String sqlServerTableName : tableNames) {
                String clickHouseTableName = CLICKHOUSE_TABLE_NAME;
                String fullClickHouseTableName = clickHouseTableName;

                // 获取SQL Server表的数据量
                int sqlServerRowCount = getTableRowCount(sqlServerConn, "SELECT COUNT(*) FROM " + sqlServerTableName);

                // 提取月份部分
                String monthPart = "20" + sqlServerTableName.substring(sqlServerTableName.length() - 4);

                // 获取ClickHouse表的数据量
                int clickHouseRowCount = getTableRowCount(clickHouseConn, "SELECT COUNT(*) FROM " + fullClickHouseTableName + " WHERE ser_num = '121' and   pt_ym = '" + monthPart + "'");

                // 写入Excel文件
                Row row = sheet.createRow(rowNum++);
                row.createCell(0).setCellValue(sqlServerTableName);
                row.createCell(1).setCellValue(fullClickHouseTableName + " WHERE ser_num = '121' and  pt_ym = '" + monthPart + "'");
                row.createCell(2).setCellValue(sqlServerRowCount);
                row.createCell(3).setCellValue(clickHouseRowCount);
                row.createCell(4).setCellValue(sqlServerRowCount == clickHouseRowCount);
            }

            // 关闭连接
            sqlServerConn.close();
            clickHouseConn.close();

            // 写入Excel文件
            try (FileOutputStream fos = new FileOutputStream("sp_count.xlsx")) {
                workbook.write(fos);
            }
            workbook.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // 读取 CSV 文件
    private static List<String> readCsvFile(String filePath) throws IOException {
        List<String> tableNames = new ArrayList<>();
        try (CSVReader reader = new CSVReader(new InputStreamReader(new FileInputStream(filePath), StandardCharsets.UTF_8))) {
            String[] line;
            while ((line = reader.readNext()) != null) {
                if (line.length > 0) {
                    tableNames.add(line[0].trim());
                }
            }
        }
        return tableNames;
    }

    // 获取表的记录数
    private static int getTableRowCount(Connection conn, String query) throws SQLException {
        try (Statement stmt = conn.createStatement(); ResultSet rs = stmt.executeQuery(query)) {
            if (rs.next()) {
                return rs.getInt(1);
            }
            return 0;
        }
    }
}
