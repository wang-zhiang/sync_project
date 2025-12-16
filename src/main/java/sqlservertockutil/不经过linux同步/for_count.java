package sqlservertockutil.不经过linux同步;




        import au.com.bytecode.opencsv.CSVReader;
        import org.apache.poi.ss.usermodel.*;
        import org.apache.poi.xssf.usermodel.XSSFWorkbook;

        import java.io.FileInputStream;
        import java.io.FileOutputStream;
        import java.io.InputStreamReader;
        import java.io.IOException;
        import java.nio.charset.StandardCharsets;
        import java.sql.*;
        import java.util.HashMap;
        import java.util.Map;

public class for_count {
    // SQL Server连接详情
    //private static final String SQL_SERVER_URL = "jdbc:sqlserver://192.168.4.218:2599;DatabaseName=datasystem";
    private static final String SQL_SERVER_URL = "jdbc:sqlserver://192.168.4.201:2422;DatabaseName=taobao_trading";
    private static final String SQL_SERVER_USER = "CHH";
    private static final String SQL_SERVER_PASSWORD = "Y1v606";

    // ClickHouse连接详情
    private static final String CLICKHOUSE_URL = "jdbc:clickhouse://hadoop110:8123/test";
    private static final String CLICKHOUSE_USER = "default";
    private static final String CLICKHOUSE_PASSWORD = "smartpath";
    private static final String CLICKHOUSE_DATABASE = "test";

    public static void main(String[] args) {
        Map<String, String> tableMappings = readCSV("D:\\wzza\\develop\\idea_project\\ceshi\\src\\main\\java\\sqlservertockutil\\不经过linux同步\\a.csv");




        for (Map.Entry<String, String> entry : tableMappings.entrySet()) {
            String sqlServerTableName = entry.getKey().trim();
            String clickHouseTableName = entry.getValue().trim();
            try {
                writeDataCountsToExcel(sqlServerTableName, clickHouseTableName);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    private static Map<String, String> readCSV(String filePath) {
        Map<String, String> tableMappings = new HashMap<>();
        try (CSVReader reader = new CSVReader(new InputStreamReader(new FileInputStream(filePath), StandardCharsets.UTF_8), ',')) {
            String[] line;
            while ((line = reader.readNext()) != null) {
                if (line.length < 2) continue; // 确保每行至少有两个值
                String sqlServerTableName = line[0].trim();
                String clickHouseTableName = line[1].trim();
                System.out.println("SQL Server表名：" + sqlServerTableName);
                System.out.println("ClickHouse表名：" + clickHouseTableName);
                tableMappings.put(sqlServerTableName, clickHouseTableName);
            }
        } catch (IOException e) {
            System.err.println("Error reading CSV file");
            e.printStackTrace();
        }
        return tableMappings;
    }

    // 获取表的记录数
    private static int getTableRowCount(Connection connection, String query) throws SQLException {
        try (Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery(query)) {
            resultSet.next();
            return resultSet.getInt(1);
        }
    }

    // 将数据量写入Excel文件
    private static void writeDataCountsToExcel(String sqlServerTableName, String clickHouseTableName) throws Exception {
        String fullClickHouseTableName = CLICKHOUSE_DATABASE +  "." + clickHouseTableName;
        // 获取SQL Server表的数据量
        int sqlServerRowCount;
        try (Connection sqlServerConnection = DriverManager.getConnection(SQL_SERVER_URL, SQL_SERVER_USER, SQL_SERVER_PASSWORD)) {
            String sqlServerCountQuery = "SELECT COUNT(*) FROM " + sqlServerTableName;
            sqlServerRowCount = getTableRowCount(sqlServerConnection, sqlServerCountQuery);
        }

        // 获取ClickHouse表的数据量
        int clickHouseRowCount;
        try (Connection clickHouseConnection = DriverManager.getConnection(CLICKHOUSE_URL, CLICKHOUSE_USER, CLICKHOUSE_PASSWORD)) {
            String clickHouseCountQuery = "SELECT COUNT(*) FROM " + fullClickHouseTableName;
            clickHouseRowCount = getTableRowCount(clickHouseConnection, clickHouseCountQuery);
        }

        // 写入Excel文件
        String excelFilePath = "data_counts.xlsx";
        Workbook workbook;
        Sheet sheet;
        boolean isNewFile = false;
        try (FileInputStream fis = new FileInputStream(excelFilePath)) {
            workbook = new XSSFWorkbook(fis);
            sheet = workbook.getSheetAt(0);
        } catch (IOException e) {
            workbook = new XSSFWorkbook();
            sheet = workbook.createSheet("DataCounts");
            isNewFile = true;
        }

        if (isNewFile) {
            Row headerRow = sheet.createRow(0);
            headerRow.createCell(0).setCellValue("SQL Server Table Name");
            headerRow.createCell(1).setCellValue("ClickHouse Table Name");
            headerRow.createCell(2).setCellValue("SQL Server Row Count");
            headerRow.createCell(3).setCellValue("ClickHouse Row Count");
            headerRow.createCell(4).setCellValue("Equal");
        }

        int lastRowNum = sheet.getLastRowNum();
        Row row = sheet.createRow(lastRowNum + 1);

        Cell sqlServerTableCell = row.createCell(0);
        sqlServerTableCell.setCellValue(sqlServerTableName);

        Cell clickHouseTableCell = row.createCell(1);
        clickHouseTableCell.setCellValue(CLICKHOUSE_DATABASE +  "." + clickHouseTableName);

        Cell sqlServerRowCountCell = row.createCell(2);
        sqlServerRowCountCell.setCellValue(sqlServerRowCount);

        Cell clickHouseRowCountCell = row.createCell(3);
        clickHouseRowCountCell.setCellValue(clickHouseRowCount);

        Cell equalCell = row.createCell(4);
        equalCell.setCellValue(sqlServerRowCount == clickHouseRowCount);

        try (FileOutputStream fos = new FileOutputStream(excelFilePath)) {
            workbook.write(fos);
        }

        workbook.close();
    }
}
