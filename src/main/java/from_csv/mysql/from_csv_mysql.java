package from_csv.mysql;

import org.apache.poi.ss.usermodel.*;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import java.io.FileInputStream;
import java.sql.*;
import java.util.*;

public class from_csv_mysql {

    private static final String JDBC_URL = "jdbc:mysql://192.168.6.101:3306/saas_price_tracking";
    private static final String USERNAME = "root";
    private static final String PASSWORD = "smartpath";

    public static void main(String[] args) {
        String excelFilePath = "D:\\wzza\\develop\\idea_project\\ceshi\\src\\main\\java\\from_csv\\mysql\\yp.xlsx";
        String tableName = "yp_itemidpool";
        boolean createTable = true; // Set to false if you want to insert into an existing table
        Map<String, String> columnMappings = new HashMap<>(); // Optional: Map Excel columns to DB columns

        // Example: If you want to map Excel columns to specific DB columns
        // columnMappings.put("ExcelColumn1", "DBColumn1");
        // columnMappings.put("ExcelColumn2", "DBColumn2");

        try {
            List<Map<String, String>> data = readExcel(excelFilePath);
            if (createTable) {
                createTable(tableName, data.get(0).keySet());
            }
            insertData(tableName, data, columnMappings);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static List<Map<String, String>> readExcel(String filePath) throws Exception {
        List<Map<String, String>> data = new ArrayList<>();
        FileInputStream file = new FileInputStream(filePath);
        Workbook workbook = new XSSFWorkbook(file);
        Sheet sheet = workbook.getSheetAt(0);
        Iterator<Row> rowIterator = sheet.iterator();

        // Read header row
        Row headerRow = rowIterator.next();
        List<String> headers = new ArrayList<>();
        for (Cell cell : headerRow) {
            headers.add(cell.getStringCellValue());
        }

        // Read data rows
        while (rowIterator.hasNext()) {
            Row row = rowIterator.next();
            Map<String, String> rowData = new HashMap<>();
            for (int i = 0; i < headers.size(); i++) {
                Cell cell = row.getCell(i, Row.MissingCellPolicy.CREATE_NULL_AS_BLANK);
                String cellValue = getCellValueAsString(cell); // 使用自定义方法获取单元格值
                rowData.put(headers.get(i), cellValue);
            }
            data.add(rowData);
        }

        workbook.close();
        file.close();
        return data;
    }

    /**
     * 获取单元格的值，如果是公式则获取计算结果
     */
    private static String getCellValueAsString(Cell cell) {
        if (cell == null) {
            return "";
        }
        switch (cell.getCellType()) {
            case STRING:
                return cell.getStringCellValue();
            case NUMERIC:
                if (DateUtil.isCellDateFormatted(cell)) {
                    return cell.getDateCellValue().toString();
                } else {
                    return String.valueOf(cell.getNumericCellValue());
                }
            case BOOLEAN:
                return String.valueOf(cell.getBooleanCellValue());
            case FORMULA:
                // 如果是公式，获取公式的计算结果
                return getFormulaCellValue(cell);
            default:
                return "";
        }
    }

    /**
     * 获取公式单元格的计算结果
     */
    private static String getFormulaCellValue(Cell cell) {
        try {
            FormulaEvaluator evaluator = cell.getSheet().getWorkbook().getCreationHelper().createFormulaEvaluator();
            CellValue cellValue = evaluator.evaluate(cell);
            switch (cellValue.getCellType()) {
                case STRING:
                    return cellValue.getStringValue();
                case NUMERIC:
                    return String.valueOf(cellValue.getNumberValue());
                case BOOLEAN:
                    return String.valueOf(cellValue.getBooleanValue());
                default:
                    return "";
            }
        } catch (Exception e) {
            e.printStackTrace();
            return "";
        }
    }

    private static void createTable(String tableName, Set<String> columns) throws SQLException {
        try (Connection conn = DriverManager.getConnection(JDBC_URL, USERNAME, PASSWORD);
             Statement stmt = conn.createStatement()) {
            StringBuilder sql = new StringBuilder("CREATE TABLE " + tableName + " (");
            for (String column : columns) {
                sql.append(column).append(" VARCHAR(255), ");
            }
            sql.setLength(sql.length() - 2); // Remove the last comma and space
            sql.append(")");
            stmt.executeUpdate(sql.toString());
        }
    }

    private static void insertData(String tableName, List<Map<String, String>> data, Map<String, String> columnMappings) throws SQLException {
        try (Connection conn = DriverManager.getConnection(JDBC_URL, USERNAME, PASSWORD)) {
            Set<String> columns = data.get(0).keySet();
            StringBuilder sql = new StringBuilder("INSERT INTO " + tableName + " (");
            for (String column : columns) {
                sql.append(columnMappings.getOrDefault(column, column)).append(", ");
            }
            sql.setLength(sql.length() - 2); // Remove the last comma and space
            sql.append(") VALUES (");
            for (int i = 0; i < columns.size(); i++) {
                sql.append("?, ");
            }
            sql.setLength(sql.length() - 2); // Remove the last comma and space
            sql.append(")");

            try (PreparedStatement pstmt = conn.prepareStatement(sql.toString())) {
                for (Map<String, String> row : data) {
                    int index = 1;
                    for (String column : columns) {
                        pstmt.setString(index++, row.get(column));
                    }
                    pstmt.executeUpdate();
                }
            }
        }
    }
}