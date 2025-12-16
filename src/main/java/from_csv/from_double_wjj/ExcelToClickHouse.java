package from_csv.from_double_wjj;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.stream.Collectors;

import org.apache.poi.ss.usermodel.*;

public class ExcelToClickHouse {
    // ClickHouse数据库连接信息
    private static final String CLICKHOUSE_URL = "jdbc:clickhouse://hadoop110:8123";
    private static final String CLICKHOUSE_USER = "default";
    private static final String CLICKHOUSE_PASSWORD = "smartpath";

    // 字段映射关系，中文字段到英文字段的映射
    private static final Map<String, String> FIELD_MAPPING = new HashMap<>();

    static {
        FIELD_MAPPING.put("序号", "serial_number");
        FIELD_MAPPING.put("日期", "date");
        FIELD_MAPPING.put("渠道", "channel");
        FIELD_MAPPING.put("行业名称", "industry_name");
        FIELD_MAPPING.put("品牌名称", "brand_name");
        FIELD_MAPPING.put("成交金额指数", "transaction_amount_index");
        FIELD_MAPPING.put("成交量指数", "transaction_volume_index");
        FIELD_MAPPING.put("访客指数", "visitor_index");
        FIELD_MAPPING.put("搜索点击指数", "search_click_index");
        FIELD_MAPPING.put("关注人数", "follower_count");
        FIELD_MAPPING.put("排名", "rank");
        FIELD_MAPPING.put("商家信息", "merchant_info");
        FIELD_MAPPING.put("访客人数", "visitor_count");
        FIELD_MAPPING.put("成交金额", "transaction_amount");
        FIELD_MAPPING.put("成交单量", "transaction_volume");
        FIELD_MAPPING.put("搜索点击人数", "search_click_visitors");
        FIELD_MAPPING.put("商品信息", "product_info");
        FIELD_MAPPING.put("商品ID", "product_id");
        FIELD_MAPPING.put("浏览量", "page_views");
        FIELD_MAPPING.put("浏览量环比（月）", "page_views_mom");
        FIELD_MAPPING.put("访客指数环比（月）", "visitor_index_mom");
        FIELD_MAPPING.put("加购人数", "add_to_cart_users");
        FIELD_MAPPING.put("加购人数环比（月）", "add_to_cart_users_mom");
        FIELD_MAPPING.put("加购商品件数", "add_to_cart_item_count");
        FIELD_MAPPING.put("加购商品件数环比（月）", "add_to_cart_item_count_mom");
        FIELD_MAPPING.put("搜索点击人气", "search_click_popularity");
        FIELD_MAPPING.put("搜索点击人气环比（月）", "search_click_popularity_mom");
        FIELD_MAPPING.put("搜索点击指数环比（月）", "search_click_index_mom");
        FIELD_MAPPING.put("搜索点击率指数", "search_click_rate_index");
        FIELD_MAPPING.put("搜索点击率指数环比（月）", "search_click_rate_index_mom");
        FIELD_MAPPING.put("成交金额指数环比（月）", "transaction_amount_index_mom");
        FIELD_MAPPING.put("成交单量指数", "transaction_volume_index");
        FIELD_MAPPING.put("成交单量指数环比（月）", "transaction_volume_index_mom");
        FIELD_MAPPING.put("成交件数指数", "transaction_item_count_index");
        FIELD_MAPPING.put("成交件数指数环比（月）", "transaction_item_count_index_mom");
        FIELD_MAPPING.put("成交转化率", "transaction_conversion_rate");
        FIELD_MAPPING.put("成交转化率环比（月）", "transaction_conversion_rate_mom");
        FIELD_MAPPING.put("成交客单件", "transaction_per_customer");
        FIELD_MAPPING.put("成交客单件环比（月）", "transaction_per_customer_mom");
        FIELD_MAPPING.put("行业店铺数", "industry_store_count");
        FIELD_MAPPING.put("行业店铺数环比（月）", "industry_store_count_mom");
        FIELD_MAPPING.put("曝光店铺数", "exposure_store_count");
        FIELD_MAPPING.put("曝光店铺数环比（月）", "exposure_store_count_mom");
        FIELD_MAPPING.put("动销店铺数", "active_store_count");
        FIELD_MAPPING.put("动销店铺数环比（月）", "active_store_count_mom");
        FIELD_MAPPING.put("曝光商品数", "exposure_product_count");
        FIELD_MAPPING.put("曝光商品数环比（月）", "exposure_product_count_mom");
        FIELD_MAPPING.put("行业", "industry");
        FIELD_MAPPING.put("子行业名称", "sub_industry_name");
        FIELD_MAPPING.put("成交金额占比", "transaction_amount_ratio");
        FIELD_MAPPING.put("成交金额增幅", "transaction_amount_growth");
        FIELD_MAPPING.put("访客数占比", "visitor_count_ratio");
        FIELD_MAPPING.put("搜索点击量占比", "search_click_volume_ratio");
        FIELD_MAPPING.put("关注人数环比（月）", "follower_count_mom");




    }

    public static void main(String[] args) {
  /*
  * ods.jd_sz_brand_list
  * ods.jd_sz_merchant_list
  * ods.jd_sz_goods_list_sku
  * ods.jd_sz_goods_list_spu
  * ods.jd_sz_industry_market
  * ods.jd_sz_sub_industry_rank
  *
  * */

        String folderPath = "D:\\wzza\\develop\\idea_project\\ceshi\\src\\main\\java\\from_csv\\from_double_wjj\\子行业排行";
        String tableName = "ods.jd_sz_sub_industry_rank";

        Path mainFolderPath = Paths.get(folderPath);
        System.out.println("Main33 folder processed: " + mainFolderPath.toAbsolutePath());

        try {

            String mainFolderName = mainFolderPath.getFileName().toString();
            Files.list(mainFolderPath)
                    .filter(Files::isDirectory) // 确保是目录
                    .forEach(subfolder -> {

                        System.out.println("Entering subfolder: " + subfolder.toAbsolutePath());
                        try {
                            List<Path> files = Files.list(subfolder)
                                    .filter(Files::isRegularFile)
                                    .filter(path -> path.toString().toLowerCase().endsWith(".xls") || path.toString().toLowerCase().endsWith(".xlsx"))
                                    .collect(Collectors.toList());

                            if (files.isEmpty()) {
                                System.out.println("No Excel files found in: " + subfolder);
                            } else {
                                files.forEach(file -> {
                                    System.out.println("Processing Excel file: " + file.getFileName());
                                    processExcelFile(file.toFile(), subfolder.getFileName().toString(),tableName,mainFolderName);
                                });
                            }
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

    private static void processExcelFile(File excelFile, String subFolderName,String tableName,String mainFolderName) {
        System.out.println("Attempting to open Excel file: " + excelFile.getAbsolutePath());
        try (FileInputStream fis = new FileInputStream(excelFile)) {
            System.out.println("Excel file opened successfully: " + excelFile.getName());
            // You may need to handle both HSSFWorkbook and XSSFWorkbook depending on the file format
            try (Workbook workbook = WorkbookFactory.create(fis)) {
                Sheet sheet = workbook.getSheetAt(0);


               // addCustomColumns(headerRow,mainFolderName, subFolderName, excelFile.getName());

                createClickHouseTableIfNeeded(excelFile, tableName);
                insertDataIntoClickHouse(mainFolderName,subFolderName,excelFile, tableName);
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




    private static void createClickHouseTableIfNeeded(File excelFile, String tableName) {
        System.out.println("Entering createClickHouseTableIfNeeded method...");
        if (excelFile.isFile() && excelFile.getName().toLowerCase().endsWith(".xlsx") || excelFile.getName().toLowerCase().endsWith(".xls")) {
            try {
                FileInputStream fis = new FileInputStream(excelFile);
                Workbook workbook = WorkbookFactory.create(fis);
                Sheet sheet = workbook.getSheetAt(0);
                Row headerRow = sheet.getRow(0);

                List<String> columns = mapColumns(headerRow);
                System.out.println("Columns: " + columns);

                String createTableSQL = buildCreateTableSQL(tableName, columns);
                System.out.println("Create table SQL: " + createTableSQL);

                try (Connection connection = DriverManager.getConnection(CLICKHOUSE_URL, CLICKHOUSE_USER, CLICKHOUSE_PASSWORD);
                     Statement statement = connection.createStatement()) {
                    statement.executeUpdate(createTableSQL);
                    System.out.println("Table created successfully: " + tableName);
                }
                fis.close();
            } catch (IOException | SQLException e) {
                e.printStackTrace();
            }
        } else {
            System.out.println("The provided file is not a valid Excel file.");
        }
    }

    private static String buildCreateTableSQL(String tableName, List<String> columns) {
        StringJoiner joiner = new StringJoiner(", ");
        for (String column : columns) {
            joiner.add(column + " String");
        }
        // 添加新列定义
        joiner.add("datatype String");
        joiner.add("industry_new String");
        joiner.add("date_new String");

        return String.format("CREATE TABLE IF NOT EXISTS %s (%s) ENGINE = MergeTree() ORDER BY %s", tableName, joiner.toString(),columns.get(0));
    }








//    private static void addCustomColumns(Row headerRow, String mainFolderName, String subFolderName, String fileName) {
//        // 添加并设置 datatype 列
//        Cell datatypeCell = headerRow.createCell(headerRow.getLastCellNum(), CellType.STRING);
//        datatypeCell.setCellValue("datatype");
//        // 添加并设置 industry_new 列
//        Cell industryNewCell = headerRow.createCell(headerRow.getLastCellNum(), CellType.STRING);
//        industryNewCell.setCellValue("industry_new");
//        // 添加并设置 date_new 列
//        Cell dateNewCell = headerRow.createCell(headerRow.getLastCellNum(), CellType.STRING);
//        dateNewCell.setCellValue("date_new");
//
//        Sheet sheet = headerRow.getSheet();
//        int numColumns = headerRow.getLastCellNum();
//        for (int i = 1; i <= sheet.getLastRowNum(); i++) {
//            Row row = sheet.getRow(i);
//            // 确保每行都有足够的单元格
//            while (row.getLastCellNum() < numColumns) {
//                row.createCell(row.getLastCellNum(), CellType.BLANK);
//            }
//            // 填充新添加的列
//            row.getCell(headerRow.getLastCellNum() - 3).setCellValue(mainFolderName);
//            row.getCell(headerRow.getLastCellNum() - 2).setCellValue(subFolderName);
//            String dateValue = "";
//            if (fileName.toLowerCase().endsWith(".xlsx")) {
//                dateValue = fileName.substring(fileName.lastIndexOf("_") + 1).replace(".xlsx", "").trim();
//            } else if (fileName.toLowerCase().endsWith(".xls")) {
//                dateValue = fileName.substring(fileName.lastIndexOf("_") + 1).replace(".xls", "").trim();
//            }
//            row.getCell(headerRow.getLastCellNum() - 1).setCellValue(dateValue);
//
////            System.out.println("Row " + i + ": datatype = " + mainFolderName
////                    + ", industry_new = " + subFolderName
////                    + ", date_new = " + dateValue);
//        }
//    }



    private static void insertDataIntoClickHouse(String mainFolderName, String subFolderName, File excelFile, String tableName) {
        System.out.println("Entering insertDataIntoClickHouse method...");
        try {
            FileInputStream fis = new FileInputStream(excelFile);
            Workbook workbook = WorkbookFactory.create(fis);
            Sheet sheet = workbook.getSheetAt(0);
            Row headerRow = sheet.getRow(0);
            List<String> columns = mapColumns(headerRow);

            //addCustomColumns(headerRow, mainFolderName, subFolderName, excelFile.getName());

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
                    // 添加新列的名称
                    insertSQL.append(", datatype, industry_new, date_new) VALUES (");
                    for (int j = 0; j < columns.size(); j++) {
                        Cell cell = row.getCell(j, Row.MissingCellPolicy.CREATE_NULL_AS_BLANK);
                        String cellValue = formatCellValue(cell);
                        insertSQL.append("'").append(cellValue.replace("'", "''")).append("'");
                        if (j < columns.size() - 1) {
                            insertSQL.append(", ");
                        }
                    }
                    // 添加新列的值

                    String dateValue = "";
                    if (excelFile.getName().toLowerCase().endsWith(".xlsx")) {
                        dateValue = excelFile.getName().substring(excelFile.getName().lastIndexOf("_") + 1).replace(".xlsx", "").trim();
                    } else if (excelFile.getName().toLowerCase().endsWith(".xls")) {
                        dateValue = excelFile.getName().substring(excelFile.getName().lastIndexOf("_") + 1).replace(".xls", "").trim();
                    }
                    insertSQL.append(", '").append(mainFolderName).append("', '").append(subFolderName).append("', '").append(dateValue).append("')");

                    System.out.println("Insert SQL: " + insertSQL);
                    statement.executeUpdate(insertSQL.toString());
                }
                System.out.println("Data inserted into table: " + tableName + " from file: " + excelFile.getName());
            }
            fis.close();
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



    private static List<String> mapColumns(Row headerRow) {
        List<String> mappedColumns = new ArrayList<>();
        for (Cell cell : headerRow) {
            String columnName = cell.getStringCellValue();
            String mappedColumn = FIELD_MAPPING.getOrDefault(columnName, columnName);
            mappedColumns.add(mappedColumn);
        }
        return mappedColumns;
    }






}
