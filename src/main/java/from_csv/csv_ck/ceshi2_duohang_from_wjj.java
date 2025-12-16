


package from_csv.csv_ck;

        import org.apache.poi.hssf.usermodel.HSSFWorkbook;
        import org.apache.poi.ss.usermodel.*;
        import org.apache.poi.xssf.usermodel.XSSFWorkbook;
        import ru.yandex.clickhouse.ClickHouseDataSource;

        import java.io.File;
        import java.io.FileInputStream;
        import java.io.FileOutputStream;
        import java.io.IOException;
        import java.sql.Connection;
        import java.sql.PreparedStatement;
        import java.sql.ResultSet;
        import java.sql.SQLException;
        import java.text.SimpleDateFormat;
        import java.util.HashMap;
        import java.util.Map;
        import java.util.regex.Matcher;
        import java.util.regex.Pattern;

/*
1.执行前先删除data_verification.xlsx
2.适用于xls 或者xlsx
3.data_verification.xlsx 是追加形式的
如果字段报空指针异常，可能是字段格式不对，把正常的格式替换过去

*/

public class ceshi2_duohang_from_wjj {
    private static final String INPUT_FOLDER = "D:\\wzza\\develop\\idea_project\\ceshi\\src\\main\\java\\from_csv\\csv_ck\\S3天猫单品-202510";
    private static final String CLICKHOUSE_URL = "jdbc:clickhouse://hadoop110:8123/ods?";
    private static final String CLICKHOUSE_USER = "default";
    private static final String CLICKHOUSE_PASSWORD = "smartpath";
    private static final String CLICKHOUSE_TABLE = "ods.shop_2510";
    private static final String OUTPUT_FILE = "D:\\wzza\\develop\\idea_project\\ceshi\\src\\main\\java\\from_csv\\csv_ck\\data_verification.xlsx";

    public static void main(String[] args) {
        Map<String, String> columnMapping = new HashMap<>();
        columnMapping.put("宝贝名称", "ItemName");
        columnMapping.put("商品类别", "Category");
        columnMapping.put("品牌", "Brand");
        columnMapping.put("售卖状态", "SalesStatus");
        columnMapping.put("参考价格", "ReferencePrice");
        columnMapping.put("成交均价", "AveragePrice");
        columnMapping.put("折扣率", "discount_rate");
        columnMapping.put("销量", "SalesVolume");
        columnMapping.put(" 销售额", "SalesAmount");
        //columnMapping.put("销售额", "SalesAmount");
        columnMapping.put("上架时间", "ListingDate");

        Pattern pattern = Pattern.compile("id=(\\d+)");

        try (Connection connection = getConnection()) {
            Workbook outputWorkbook;
            Sheet outputSheet;
            int outputRowNum;

            File outputFile = new File(OUTPUT_FILE);
            if (outputFile.exists()) {
                try (FileInputStream existingFis = new FileInputStream(outputFile)) {
                    outputWorkbook = new XSSFWorkbook(existingFis);
                    outputSheet = outputWorkbook.getSheetAt(0);
                    outputRowNum = outputSheet.getLastRowNum() + 1;
                }
            } else {
                outputWorkbook = new XSSFWorkbook();
                outputSheet = outputWorkbook.createSheet("Data Verification");
                Row headerRow = outputSheet.createRow(0);
                headerRow.createCell(0).setCellValue("Shop");
                headerRow.createCell(1).setCellValue("XLSX Data Count");
                headerRow.createCell(2).setCellValue("CK Data Count");
                headerRow.createCell(3).setCellValue("Is Count Equal");
                headerRow.createCell(4).setCellValue("Reason");
                outputRowNum = 1;
            }

            File folder = new File(INPUT_FOLDER);
            File[] listOfFiles = folder.listFiles((dir, name) -> name.toLowerCase().endsWith(".xls") || name.toLowerCase().endsWith(".xlsx"));

            if (listOfFiles != null) {
                for (File file : listOfFiles) {
                    System.out.println("Processing file: " + file.getName());
                    try (FileInputStream fis = new FileInputStream(file);
                         Workbook workbook = getWorkbook(fis, file.getName())) {

                        for (int sheetIndex = 0; sheetIndex < workbook.getNumberOfSheets(); sheetIndex++) {
                            Sheet sheet = workbook.getSheetAt(sheetIndex);
                            String sheetName = sheet.getSheetName();
                            System.out.println("Processing sheet: " + sheetName);

                            Row outputRow = outputSheet.createRow(outputRowNum++);
                            outputRow.createCell(0).setCellValue(sheetName);

                            if (sheet == null || sheet.getLastRowNum() == 0) {
                                System.out.println("Skipping empty sheet: " + sheetName);
                                outputRow.createCell(1).setCellValue(0);
                                outputRow.createCell(2).setCellValue(0);
                                outputRow.createCell(3).setCellValue(false);
                                outputRow.createCell(4).setCellValue("Sheet is empty");
                                continue;
                            }

                            Row header = sheet.getRow(0);
                            if (header == null) {
                                System.out.println("Skipping sheet without header: " + sheetName);
                                outputRow.createCell(1).setCellValue(0);
                                outputRow.createCell(2).setCellValue(0);
                                outputRow.createCell(3).setCellValue(false);
                                outputRow.createCell(4).setCellValue("Sheet has no header");
                                continue;
                            }

                            int xlsxDataCount = sheet.getLastRowNum();
                            outputRow.createCell(1).setCellValue(xlsxDataCount);

                            StringBuilder sql = new StringBuilder("INSERT INTO " + CLICKHOUSE_TABLE + " (");

                            for (int i = 1; i < header.getPhysicalNumberOfCells(); i++) {
                                String columnName = i == 1 ? "宝贝名称" : header.getCell(i).getStringCellValue().trim();
                                if (columnMapping.containsKey(columnName)) {
                                    sql.append(columnMapping.get(columnName)).append(", ");
                                }
                            }

                            sql.append("itemid, shop) VALUES (");

                            for (int i = 1; i < header.getPhysicalNumberOfCells(); i++) {
                                String columnName = i == 1 ? "宝贝名称" : header.getCell(i).getStringCellValue().trim();
                                if (columnMapping.containsKey(columnName)) {
                                    sql.append("?, ");
                                }
                            }
                            sql.append("?, ?)");

                            System.out.println("SQL Query: " + sql.toString());

                            try (PreparedStatement statement = connection.prepareStatement(sql.toString())) {
                                SimpleDateFormat sdf = new SimpleDateFormat("yyyy/MM/dd");
                                for (int i = 1; i <= sheet.getLastRowNum(); i++) {
                                    Row row = sheet.getRow(i);
                                    if (row == null || isRowEmpty(row)) {
                                        System.out.println("Skipping empty row: " + i + " in sheet: " + sheetName);
                                        continue;
                                    }

                                    Cell itemNameCell = row.getCell(1);
                                    if (itemNameCell == null || itemNameCell.toString().trim().isEmpty()) {
                                        System.out.println("Skipping row: " + i + " in sheet: " + sheetName + " due to empty item name");
                                        continue;
                                    }

                                    int columnCounter = 1;
                                    String itemid = null;

                                    for (int j = 1; j < row.getPhysicalNumberOfCells(); j++) {
                                        Cell cell = row.getCell(j);
                                        String columnName = j == 1 ? "宝贝名称" : header.getCell(j).getStringCellValue().trim();
                                        if (cell != null) {
                                            String cellValue = cell.toString().replaceAll("[\n\r\t]", " ").trim();
                                            if (j == 1) {
                                                Hyperlink hyperlink = cell.getHyperlink();
                                                if (hyperlink != null) {
                                                    Matcher matcher = pattern.matcher(hyperlink.getAddress());
                                                    if (matcher.find()) {
                                                        itemid = matcher.group(1);
                                                    }
                                                }
                                            }
                                            if (cell.getCellType() == CellType.NUMERIC && DateUtil.isCellDateFormatted(cell)) {
                                                cellValue = sdf.format(cell.getDateCellValue());
                                            } else {
                                                cellValue = cellValue.replace(",", "");
                                            }
                                            if (columnMapping.containsKey(columnName)) {
                                                statement.setString(columnCounter++, cellValue);
                                            } else {
                                                System.out.println("Column not found in mapping when setting values: " + columnName);
                                            }
                                        }
                                    }

                                    if (itemid != null) {
                                        statement.setString(columnCounter++, itemid);
                                    } else {
                                        statement.setString(columnCounter++, "");
                                    }

                                    statement.setString(columnCounter, sheetName);

                                    if (columnCounter != header.getPhysicalNumberOfCells() + 1) {
                                        System.out.println("ColumnCounter: " + columnCounter + ", Expected: " + (header.getPhysicalNumberOfCells() + 1));
                                    }

                                    statement.addBatch();
                                }
                                statement.executeBatch();
                                System.out.println("Data successfully inserted into ClickHouse for sheet: " + sheetName);
                            }

                            int ckDataCount = getClickHouseDataCount(connection, sheetName);
                            outputRow.createCell(2).setCellValue(ckDataCount);

                            boolean isCountEqual = xlsxDataCount == ckDataCount && ckDataCount > 0;
                            outputRow.createCell(3).setCellValue(isCountEqual);

                            if (ckDataCount == 0) {
                                outputRow.createCell(4).setCellValue("No data imported to ClickHouse");
                            } else if (!isCountEqual) {
                                outputRow.createCell(4).setCellValue("Data count mismatch");
                            }
                        }
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }

            try (FileOutputStream fos = new FileOutputStream(OUTPUT_FILE)) {
                outputWorkbook.write(fos);
            }
            System.out.println("Data verification Excel file generated successfully.");

        } catch (SQLException | IOException e) {
            e.printStackTrace();
        }
    }

    private static boolean isRowEmpty(Row row) {
        for (int c = row.getFirstCellNum(); c < row.getLastCellNum(); c++) {
            Cell cell = row.getCell(c);
            if (cell != null && cell.getCellType() != CellType.BLANK) {
                return false;
            }
        }
        return true;
    }

    private static Connection getConnection() throws SQLException {
        ClickHouseDataSource dataSource = new ClickHouseDataSource(CLICKHOUSE_URL);
        return dataSource.getConnection(CLICKHOUSE_USER, CLICKHOUSE_PASSWORD);
    }

    private static int getClickHouseDataCount(Connection connection, String sheetName) throws SQLException {
        String query = "SELECT COUNT(*)  FROM " + CLICKHOUSE_TABLE + " WHERE shop = ?";
        try (PreparedStatement stmt = connection.prepareStatement(query)) {
            stmt.setString(1, sheetName);
            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    return rs.getInt(1);
                }
            }
        }
        return 0;
    }

    private static Workbook getWorkbook(FileInputStream fis, String filePath) throws IOException {
        if (filePath.toLowerCase().endsWith("xlsx")) {
            return new XSSFWorkbook(fis);
        } else if (filePath.toLowerCase().endsWith("xls")) {
            return new HSSFWorkbook(fis);
        } else {
            throw new IllegalArgumentException("The specified file is not Excel file");
        }
    }
}
