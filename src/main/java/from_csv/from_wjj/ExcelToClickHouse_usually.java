package from_csv.from_wjj;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;

import org.apache.poi.ss.usermodel.*;

public class ExcelToClickHouse_usually {
    // ClickHouse数据库连接信息
    private static final String CLICKHOUSE_URL = "jdbc:clickhouse://hadoop110:8123";
    private static final String CLICKHOUSE_USER = "default";
    private static final String CLICKHOUSE_PASSWORD = "smartpath";

    // 字段映射关系，中文字段到英文字段的映射
    private static final Map<String, String> FIELD_MAPPING = new HashMap<>();

    static {
        FIELD_MAPPING.put("Year", "year");
        FIELD_MAPPING.put("YM", "ym");
        FIELD_MAPPING.put("Channel", "channel");
        FIELD_MAPPING.put("类别名称", "category_name");
        FIELD_MAPPING.put("一级类目", "first_category");
        FIELD_MAPPING.put("二级类目", "second_category");
        FIELD_MAPPING.put("三级类目", "third_category");
        FIELD_MAPPING.put("Value", "value");
        FIELD_MAPPING.put("Value000", "value000");
        FIELD_MAPPING.put("Volume", "volume");
        FIELD_MAPPING.put("Volume000", "volume000");
    }

    public static void main(String[] args) {
        // 文件夹路径
        String folderPath = "D:\\wzza\\develop\\idea_project\\ceshi\\src\\main\\java\\from_csv\\from_wjj\\临时";
        String tableName = "dwd.source3_industry_table";

        // 获取文件夹中的所有Excel文件
        try {
            Files.list(Paths.get(folderPath))
                    .filter(Files::isRegularFile) // 确保是文件
                    .filter(path -> path.toString().endsWith(".xlsx")) // 过滤出 .xlsx 文件
                    .forEach(path -> {
                        String fileName = path.getFileName().toString();
                        System.out.println("Processing file: " + fileName);
                        // 在这里调用处理Excel文件的方法
                        System.out.println("File path: " + path.toFile().getAbsolutePath());
                        processExcelFile(path.toFile(), tableName);
                    });
        } catch (IOException e) {
            System.out.println("Error processing folder: " + folderPath);
            e.printStackTrace();
        }
    }

    private static void processExcelFile(File excelFile, String tableName) {
        System.out.println("Attempting to open Excel file: " + excelFile.getAbsolutePath());
        try (FileInputStream fis = new FileInputStream(excelFile)) {
            System.out.println("Excel file opened successfully: " + excelFile.getName());
            try {
                Workbook workbook = WorkbookFactory.create(fis);
                System.out.println("Workbook created successfully.");
                Sheet sheet = workbook.getSheetAt(0);
                System.out.println("Sheet fetched: " + sheet.getSheetName());
                Row headerRow = sheet.getRow(0);
                System.out.println("Header row fetched: " + headerRow);

                // 创建ClickHouse表并插入数据
                createClickHouseTableIfNeeded(sheet, tableName);
                insertDataIntoClickHouse(sheet, tableName);
                System.out.println("Excel file processed successfully: " + excelFile.getName());
            } catch (Exception e) {
                System.out.println("Error processing workbook: " + excelFile.getName());
                e.printStackTrace();
            }
        } catch (IOException e) {
            System.out.println("Failed to open file: " + excelFile.getName());
            e.printStackTrace();
        }
    }

    private static void createClickHouseTableIfNeeded(Sheet sheet, String tableName) {
        System.out.println("Entering createClickHouseTableIfNeeded method...");
        try {
            Row headerRow = sheet.getRow(0);
            if (headerRow == null) {
                System.out.println("Header row is null.");
                return;
            }
            List<String> columns = mapColumns(headerRow);
            System.out.println("Columns: " + columns);

            String createTableSQL = buildCreateTableSQL(tableName, columns);
            System.out.println("Create table SQL: " + createTableSQL);

            try (Connection connection = DriverManager.getConnection(CLICKHOUSE_URL, CLICKHOUSE_USER, CLICKHOUSE_PASSWORD);
                 Statement statement = connection.createStatement()) {
                statement.executeUpdate(createTableSQL);
                System.out.println("Table created successfully: " + tableName);
            }
        } catch (SQLException e) {
            System.out.println("Error in createClickHouseTableIfNeeded");
            e.printStackTrace();
        }
    }

    private static String buildCreateTableSQL(String tableName, List<String> columns) {
        StringJoiner joiner = new StringJoiner(", ");
        for (String column : columns) {
            joiner.add(column + " String");
        }
        return String.format("CREATE TABLE IF NOT EXISTS %s (%s) ENGINE = MergeTree() ORDER BY %s", tableName, joiner.toString(), columns.get(0));
    }

    private static void insertDataIntoClickHouse(Sheet sheet, String tableName) {
        System.out.println("Entering insertDataIntoClickHouse method...");
        try {
            Row headerRow = sheet.getRow(0);
            if (headerRow == null) {
                System.out.println("Header row is null.");
                return;
            }
            List<String> columns = mapColumns(headerRow);

            try (Connection connection = DriverManager.getConnection(CLICKHOUSE_URL, CLICKHOUSE_USER, CLICKHOUSE_PASSWORD);
                 Statement statement = connection.createStatement()) {
                for (int i = 1; i <= sheet.getLastRowNum(); i++) {
                    Row row = sheet.getRow(i);
                    if (row == null) {
                        continue;
                    }
                    StringBuilder insertSQL = new StringBuilder("INSERT INTO " + tableName + " (");
                    for (int j = 0; j < columns.size(); j++) {
                        insertSQL.append(columns.get(j));
                        if (j < columns.size() - 1) {
                            insertSQL.append(", ");
                        }
                    }
                    insertSQL.append(") VALUES (");
                    for (int j = 0; j < columns.size(); j++) {
                        Cell cell = row.getCell(j, Row.MissingCellPolicy.CREATE_NULL_AS_BLANK);
                        String cellValue = formatCellValue(cell);
                        insertSQL.append("'").append(cellValue.replace("'", "''")).append("'");
                        if (j < columns.size() - 1) {
                            insertSQL.append(", ");
                        }
                    }
                    insertSQL.append(")");
                    System.out.println("Insert SQL: " + insertSQL);
                    statement.executeUpdate(insertSQL.toString());
                }
                System.out.println("Data inserted into table: " + tableName + " from file: " + sheet.getSheetName());
            }
        } catch (SQLException e) {
            System.out.println("Error in insertDataIntoClickHouse");
            e.printStackTrace();
        }
    }

    // Helper method to format cell values properly and remove special characters
    private static String formatCellValue(Cell cell) {
        if (cell == null || cell.getCellType() == CellType.BLANK) {
            return "";  // 处理空单元格，返回空字符串
        }
        DataFormatter formatter = new DataFormatter();  // 创建一个数据格式化器
        String cellValue = formatter.formatCellValue(cell);  // 使用DataFormatter来获取单元格的文字，这会保持原样
        // 清除所有换行符、制表符和回车符，以防止错列
        return cellValue.replace("\n", "").replace("\r", "");
    }

    private static List<String> mapColumns(Row headerRow) {
        List<String> mappedColumns = new ArrayList<>();
        for (Cell cell : headerRow) {
            if (cell == null) {
                System.out.println("Null cell found in header row.");
                continue;
            }
            String columnName = cell.getStringCellValue();
            String mappedColumn = FIELD_MAPPING.getOrDefault(columnName, columnName);
            mappedColumns.add(mappedColumn);
            System.out.println("Mapped column: " + columnName + " -> " + mappedColumn);
        }
        return mappedColumns;
    }
}
