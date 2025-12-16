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
import java.text.SimpleDateFormat;
import java.util.*;

import org.apache.poi.ss.usermodel.*;

public class ExcelToClickHouse {
    // ClickHouse数据库连接信息
    private static final String CLICKHOUSE_URL = "jdbc:clickhouse://hadoop110:8123";
    private static final String CLICKHOUSE_USER = "default";
    private static final String CLICKHOUSE_PASSWORD = "smartpath";

    // 字段映射关系，中文字段到英文字段的映射
    private static final Map<String, String> FIELD_MAPPING = new HashMap<>();

    static {
        FIELD_MAPPING.put("日期", "date");
        FIELD_MAPPING.put("分类名称", "category_name");
        FIELD_MAPPING.put("交易指数", "transaction_index");
        FIELD_MAPPING.put("交易指数同比", "transaction_index_yoy");
        FIELD_MAPPING.put("交易指数占比", "transaction_index_ratio");
        FIELD_MAPPING.put("客群指数", "audience_index");
        FIELD_MAPPING.put("客群指数同比", "audience_index_yoy");
        FIELD_MAPPING.put("客群指数占比", "audience_index_ratio");
        FIELD_MAPPING.put("搜索人气", "search_popularity");
        FIELD_MAPPING.put("平台", "platform");
        FIELD_MAPPING.put("店铺名称", "store_name");
        FIELD_MAPPING.put("店铺ID", "store_id");
        FIELD_MAPPING.put("店铺链接", "store_link");
        FIELD_MAPPING.put("类目名称", "category_name");
        FIELD_MAPPING.put("行业排名", "industry_rank");
        FIELD_MAPPING.put("趋势", "trend");
        FIELD_MAPPING.put("交易增长幅度", "transaction_growth");
        FIELD_MAPPING.put("支付转化指数", "conversion_index");
        FIELD_MAPPING.put("品牌名称", "brand_name");
        FIELD_MAPPING.put("品牌ID", "brand_id");
        FIELD_MAPPING.put("商品名称", "product_name");
        FIELD_MAPPING.put("商品ID", "product_id");
        FIELD_MAPPING.put("所属店铺", "store_name");
        FIELD_MAPPING.put("是否新品", "is_new_product");


    }

    public static void main(String[] args) {

    // 文件夹路径
    String folderPath = "D:\\wzza\\develop\\idea_project\\ceshi\\src\\main\\java\\from_csv\\from_wjj\\品牌";
    String tableName = "ods.ods_xl_brand";

        // 获取大文件夹中所有子文件夹
        try {
            Files.list(Paths.get(folderPath))
                    .filter(Files::isDirectory) // 确保是目录
                    .forEach(subfolder -> {
                        try {
                            // 遍历每个子文件夹中的Excel文件
                            Files.list(subfolder)
                                    .filter(Files::isRegularFile)
                                    .filter(path -> path.toString().endsWith(".xlsx"))
                                    .forEach(path -> {
                                        String fileName = path.getFileName().toString();
                                        System.out.println("Processing file: " + fileName);
                                        // 在这里调用处理Excel文件的方法
                                        System.out.println(path.toFile());
                                        createClickHouseTableIfNeeded(path.toFile(), tableName);
                                        insertDataIntoClickHouse(path.toFile(), tableName);
                                        //System.out.println("Processing complete for file: " + fileName);
                                    });
                        } catch (IOException e) {
                            System.out.println("Error processing subfolder: " + subfolder);
                            e.printStackTrace();
                        }
                    });
        } catch (IOException e) {
            System.out.println("Error processing main folder: " + folderPath);
            e.printStackTrace();
        }
    }


    private static void createClickHouseTableIfNeeded(File excelFile, String tableName) {
        System.out.println("Entering createClickHouseTableIfNeeded method...1");
        if (excelFile.isFile() && excelFile.getName().toLowerCase().endsWith(".xlsx")) {
            try {
                System.out.println("Processing Excel file: " + excelFile.getName());

                FileInputStream fis = new FileInputStream(excelFile);
                Workbook workbook = WorkbookFactory.create(fis);
                Sheet sheet = workbook.getSheetAt(0);
                Row headerRow = sheet.getRow(0);
                System.out.println("Header row: " + headerRow);

                // 检查是否包含平台列，如果不包含则添加
                if (!headerContainsColumn(headerRow, "平台")) {
                    addPlatformColumn(headerRow, FIELD_MAPPING.get("平台"));
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

            } catch (IOException | SQLException e) {
                e.printStackTrace();
            }
        } else {
            System.out.println("The provided file is not a valid Excel file.");
        }
    }





    private static void addPlatformColumn(Row headerRow, String columnName) {
        // 添加平台列名
        Cell platformCell = headerRow.createCell(headerRow.getLastCellNum(), CellType.STRING);
        platformCell.setCellValue(columnName);

        // 填充平台列的默认值为"天猫"
        Sheet sheet = headerRow.getSheet();
        int numColumns = headerRow.getLastCellNum();  // 获取列总数，包括新添加的
        for (int i = 1; i <= sheet.getLastRowNum(); i++) {
            Row row = sheet.getRow(i);
            // 确保每行都有足够的单元格
            if (row.getLastCellNum() < numColumns) {
                for (int j = row.getLastCellNum(); j < numColumns; j++) {
                    row.createCell(j, CellType.BLANK);  // 添加必要的空单元格
                }
            }
            Cell newCell = row.createCell(numColumns - 1, CellType.STRING);  // 创建新单元格
            newCell.setCellValue("淘宝");  // 设置默认值
        }
    }

    private static void insertDataIntoClickHouse(File excelFile, String tableName) {
        System.out.println("Entering insertDataIntoClickHouse method...");
        try {
            FileInputStream fis = new FileInputStream(excelFile);
            Workbook workbook = WorkbookFactory.create(fis);
            Sheet sheet = workbook.getSheetAt(0);
            Row headerRow = sheet.getRow(0);
            List<String> columns = mapColumns(headerRow);

            // 如果没有平台列，则添加并填充为“天猫”
            if (!columns.contains(FIELD_MAPPING.get("平台"))) {
                addPlatformColumn(headerRow, FIELD_MAPPING.get("平台"));
                columns = mapColumns(headerRow); // 更新列名列表
            }

            try (Connection connection = DriverManager.getConnection(CLICKHOUSE_URL, CLICKHOUSE_USER, CLICKHOUSE_PASSWORD);
                 Statement statement = connection.createStatement()) {
                for (int i = 1; i <= sheet.getLastRowNum(); i++) {
                    Row row = sheet.getRow(i);
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
                System.out.println("Data inserted into table: " + tableName + " from file: " + excelFile.getName());
            }
        } catch (IOException | SQLException e) {
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







    private static boolean headerContainsColumn(Row headerRow, String columnName) {
        for (Cell cell : headerRow) {
            if (cell.getStringCellValue().equals(columnName)) {
                return true;
            }
        }
        return false;
    }

    private static void addPlatformColumn(Row headerRow) {
        Cell platformCell = headerRow.createCell(headerRow.getLastCellNum());
        platformCell.setCellValue("平台");
    }

    private static List<String> mapColumns(Row headerRow) {
        List<String> mappedColumns = new ArrayList<>();
        for (Cell cell : headerRow) {
            String columnName = cell.getStringCellValue();
            String mappedColumn = FIELD_MAPPING.getOrDefault(columnName, columnName);
            mappedColumns.add(mappedColumn);
        }
        return mappedColumns;
    }

    private static String buildCreateTableSQL(String tableName, List<String> columns) {
        StringJoiner joiner = new StringJoiner(", ");
        for (String column : columns) {
            joiner.add(column + " String");
        }

        return String.format("CREATE TABLE IF NOT EXISTS %s (%s) ENGINE = MergeTree() ORDER BY %s", tableName, joiner.toString(), columns.get(0));
    }



}
