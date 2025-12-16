package from_csv.mysql;

import org.apache.poi.ss.usermodel.*;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import java.io.File;
import java.io.FileInputStream;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.*;

public class ExcelToMySQL {
    // Database credentials
    private static final String DB_URL = "jdbc:mysql://192.168.6.101:3306/saas_price_tracking";
    private static final String USER = "root";
    private static final String PASSWORD = "smartpath";

    public static void main(String[] args) throws Exception {
        String excelFilePath = "D:\\wzza\\develop\\idea_project\\ceshi\\src\\main\\java\\from_csv\\mysql\\yp2_0313_new.xlsx";
        String tableName = "yp2";  // Change this to the table name in MySQL
        boolean autoCreateTable = true; // Set to true to auto-create table
        boolean insertIntoExistingTable = false; // Set to true to insert into an existing table

        List<String> columns = new ArrayList<>(); // If you want to specify columns, you can list them here

        readExcelAndInsertIntoMySQL(excelFilePath, tableName, autoCreateTable, insertIntoExistingTable, columns);
    }

    public static void readExcelAndInsertIntoMySQL(String excelFilePath, String tableName, boolean autoCreateTable, boolean insertIntoExistingTable, List<String> columns) throws Exception {
        // Step 1: Read Excel File
        FileInputStream fis = new FileInputStream(new File(excelFilePath));
        Workbook workbook = new XSSFWorkbook(fis);
        Sheet sheet = workbook.getSheetAt(0);

        FormulaEvaluator formulaEvaluator = workbook.getCreationHelper().createFormulaEvaluator();
        List<Map<String, String>> dataList = new ArrayList<>();

        Iterator<Row> rowIterator = sheet.iterator();
        Row headerRow = rowIterator.next(); // Get header row
        List<String> columnNames = new ArrayList<>();

        // Read header row and populate columnNames
        for (Cell cell : headerRow) {
            columnNames.add(cell.getStringCellValue());
        }

        // Read the rest of the data
        while (rowIterator.hasNext()) {
            Row row = rowIterator.next();
            Map<String, String> dataMap = new HashMap<>();
            for (int i = 0; i < columnNames.size(); i++) {
                Cell cell = row.getCell(i);
                String cellValue = "";

                if (cell != null) {
                    switch (formulaEvaluator.evaluateInCell(cell).getCellType()) {
                        case NUMERIC:
                            if (DateUtil.isCellDateFormatted(cell)) {
                                // Handle date cells
                                cellValue = formatDate(cell.getDateCellValue());
                            } else {
                                // Handle numbers, ensuring they don't appear in scientific notation
                                cellValue = cleanNumericValue(cell.getNumericCellValue());
                            }
                            break;
                        case STRING:
                            cellValue = cell.getStringCellValue();
                            break;
                        case BOOLEAN:
                            cellValue = String.valueOf(cell.getBooleanCellValue());
                            break;
                        case FORMULA:
                            // Handle formula cells
                            cellValue = cleanNumericValue(formulaEvaluator.evaluateInCell(cell).getNumericCellValue());
                            break;
                    }
                }

                // Remove commas or other unwanted characters in numerical values
                dataMap.put(columnNames.get(i), cellValue);
            }
            dataList.add(dataMap);
        }
        workbook.close();

        // Step 2: Connect to MySQL and process
        Connection conn = DriverManager.getConnection(DB_URL, USER, PASSWORD);

        // If autoCreateTable is true, create table if it doesn't exist
        if (autoCreateTable) {
            createTableIfNotExists(conn, tableName, columnNames);
        }

        // Step 3: Insert data
        if (insertIntoExistingTable) {
            insertIntoExistingTable(conn, tableName, columnNames, dataList);
        } else {
            insertIntoNewTable(conn, tableName, columnNames, dataList);
        }

        conn.close();
    }

    private static String formatDate(java.util.Date date) {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        return sdf.format(date);
    }

    private static String cleanNumericValue(double value) {
        // Remove commas or other unwanted characters
        String valueStr = String.valueOf(value);
        valueStr = valueStr.replace(",", ""); // Remove commas
        // Check if the value is in scientific notation and format it
        if (valueStr.contains("E")) {
            return String.format("%.0f", value); // Convert to normal number representation
        }
        return valueStr;
    }

    private static void createTableIfNotExists(Connection conn, String tableName, List<String> columnNames) throws SQLException {
        StringBuilder createTableQuery = new StringBuilder("CREATE TABLE IF NOT EXISTS " + tableName + " (");

        for (String column : columnNames) {
            createTableQuery.append(column).append(" NVARCHAR(500), ");
        }
        createTableQuery.delete(createTableQuery.length() - 2, createTableQuery.length());
        createTableQuery.append(")");

        Statement stmt = conn.createStatement();
        stmt.executeUpdate(createTableQuery.toString());
    }

    private static void insertIntoExistingTable(Connection conn, String tableName, List<String> columnNames, List<Map<String, String>> dataList) throws SQLException {
        String insertSQL = buildInsertSQL(tableName, columnNames);
        try (PreparedStatement pstmt = conn.prepareStatement(insertSQL)) {
            conn.setAutoCommit(false);

            for (Map<String, String> data : dataList) {
                int index = 1;
                for (String column : columnNames) {
                    pstmt.setString(index++, data.getOrDefault(column, ""));
                }
                pstmt.addBatch();
            }

            pstmt.executeBatch();
            conn.commit();
        }
    }

    private static void insertIntoNewTable(Connection conn, String tableName, List<String> columnNames, List<Map<String, String>> dataList) throws SQLException {
        String insertSQL = buildInsertSQL(tableName, columnNames);
        try (PreparedStatement pstmt = conn.prepareStatement(insertSQL)) {
            conn.setAutoCommit(false);

            for (Map<String, String> data : dataList) {
                int index = 1;
                for (String column : columnNames) {
                    pstmt.setString(index++, data.getOrDefault(column, ""));
                }
                pstmt.addBatch();
            }

            pstmt.executeBatch();
            conn.commit();
        }
    }

    private static String buildInsertSQL(String tableName, List<String> columnNames) {
        StringBuilder insertSQL = new StringBuilder("INSERT INTO " + tableName + " (");
        for (String column : columnNames) {
            insertSQL.append(column).append(", ");
        }
        insertSQL.delete(insertSQL.length() - 2, insertSQL.length());
        insertSQL.append(") VALUES (");

        for (int i = 0; i < columnNames.size(); i++) {
            insertSQL.append("?, ");
        }
        insertSQL.delete(insertSQL.length() - 2, insertSQL.length());
        insertSQL.append(")");

        return insertSQL.toString();
    }
}
