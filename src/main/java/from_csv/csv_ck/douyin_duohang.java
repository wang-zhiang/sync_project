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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

//原代码：for (int i = 2; ...) （因为 row 0,1 是标题）
//新代码：for (int i = 1; ...) （因为 row 0 是标题，数据从 row 1 开始）

//注意： 如果标题占了一行 ，设置i =1  。 如果标题占了两行，设置 i  = 2;

public class douyin_duohang {
    private static final String INPUT_FOLDER = "D:\\wzza\\develop\\idea_project\\ceshi\\src\\main\\java\\from_csv\\csv_ck\\抖音";
    private static final String CLICKHOUSE_URL = "jdbc:clickhouse://hadoop110:8123/ods?";
    private static final String CLICKHOUSE_USER = "default";
    private static final String CLICKHOUSE_PASSWORD = "smartpath";
    private static final String CLICKHOUSE_TABLE = "ods.dy_shop_20260104";
    private static final String OUTPUT_FILE = "D:\\wzza\\develop\\idea_project\\ceshi\\src\\main\\java\\from_csv\\csv_ck\\data_verification.xlsx";

    public static void main(String[] args) {
        Pattern patternId = Pattern.compile("id=(\\d+)");

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

                            if (sheet == null || sheet.getLastRowNum() < 2) {
                                System.out.println("Skipping empty sheet: " + sheetName);
                                continue;
                            }

                            String sql = "INSERT INTO " + CLICKHOUSE_TABLE + " (ItemName, ProductCategory, SalesStatus, Brand, ReferencePrice, AvgTransactionPrice, PostReturnSalesVolume, PostReturnSalesAmount, itemid, shop) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
                            int xlsxDataCount = 0;

                            try (PreparedStatement statement = connection.prepareStatement(sql)) {
                                int processedRows = 0;

                                // 循环步长为3 (每3行为一组)
                                for (int i = 1; i <= sheet.getLastRowNum(); i += 3) {

                                    // rowTop (第1行): 包含 B列~H列 的数据 (因为是合并单元格，值在左上角)
                                    Row rowTop = sheet.getRow(i);

                                    // rowBottom (第3行): 包含 A列 (宝贝名称) 的数据 (你说数据在第3行)
                                    Row rowBottom = sheet.getRow(i + 2);

                                    // 如果两行都为空，跳过
                                    if (rowTop == null && rowBottom == null) continue;

                                    // 检查是否是汇总行
                                    // 汇总通常写在第一列，所以检查 rowBottom 的 A列，或者 rowTop 的 A列
                                    if (rowBottom != null) {
                                        String val = getCellValue(rowBottom.getCell(0));
                                        if (val.contains("本页合计") || val.contains("全部合计")) continue;
                                    }
                                    if (rowTop != null) {
                                        String val = getCellValue(rowTop.getCell(0));
                                        if (val.contains("本页合计") || val.contains("全部合计")) continue;
                                    }

                                    // --- 1. 宝贝名称 & ID (A列) -> 读 RowBottom (第3行) ---
                                    String itemName = "";
                                    String itemid = "";
                                    if (rowBottom != null) {
                                        Cell nameCell = rowBottom.getCell(0); // A列
                                        itemName = getCellValue(nameCell);

                                        if (nameCell != null) {
                                            Hyperlink link = nameCell.getHyperlink();
                                            if (link != null) {
                                                Matcher m = patternId.matcher(link.getAddress());
                                                if (m.find()) itemid = m.group(1);
                                            }
                                        }
                                    }

                                    // 如果名字为空，跳过
                                    if (itemName.isEmpty()) continue;

                                    // --- 2. 其他所有列 (B-H列) -> 读 RowTop (第1行) ---
                                    // 这是一个合并了3行的单元格，POI 只能在第一行读到值
                                    String productCategory = "";
                                    String salesStatus = "";
                                    String brand = "";
                                    String referencePrice = "";
                                    String avgTransactionPrice = "";
                                    String postReturnSalesVolume = "";
                                    String postReturnSalesAmount = "";

                                    if (rowTop != null) {
                                        productCategory = getCellValue(rowTop.getCell(1));       // B列
                                        salesStatus = getCellValue(rowTop.getCell(2));           // C列
                                        brand = getCellValue(rowTop.getCell(3));                 // D列
                                        referencePrice = getCellValue(rowTop.getCell(4));        // E列
                                        avgTransactionPrice = getCellValue(rowTop.getCell(5));   // F列
                                        postReturnSalesVolume = getCellValue(rowTop.getCell(6)); // G列
                                        postReturnSalesAmount = getCellValue(rowTop.getCell(7)); // H列
                                    }

                                    // 调试打印：验证是否正确对应
                                    if (processedRows == 0) {
                                        System.out.println("✅ Debug Record (Row " + i + "):");
                                        System.out.println("   Name (From Row 3): " + itemName);
                                        System.out.println("   Category (From Row 1): " + productCategory);
                                        System.out.println("   Brand (From Row 1): " + brand);
                                        System.out.println("   Price (From Row 1): " + avgTransactionPrice);
                                    }

                                    // 插入数据库
                                    statement.setString(1, itemName);
                                    statement.setString(2, productCategory);
                                    statement.setString(3, salesStatus);
                                    statement.setString(4, brand);
                                    statement.setString(5, cleanNumber(referencePrice));
                                    statement.setString(6, cleanNumber(avgTransactionPrice));
                                    statement.setString(7, cleanNumber(postReturnSalesVolume));
                                    statement.setString(8, cleanNumber(postReturnSalesAmount));
                                    statement.setString(9, itemid);
                                    statement.setString(10, sheetName);

                                    statement.addBatch();
                                    processedRows++;
                                    xlsxDataCount++;

                                    if (processedRows % 1000 == 0) {
                                        statement.executeBatch();
                                        System.out.println("Processed " + processedRows + " rows...");
                                    }
                                }
                                if (processedRows > 0) statement.executeBatch();
                            }

                            // 验证统计
                            outputRow.createCell(1).setCellValue(xlsxDataCount);
                            int ckDataCount = getClickHouseDataCount(connection, sheetName);
                            outputRow.createCell(2).setCellValue(ckDataCount);
                            boolean isCountEqual = xlsxDataCount == ckDataCount && ckDataCount > 0;
                            outputRow.createCell(3).setCellValue(isCountEqual);
                            if (!isCountEqual) {
                                outputRow.createCell(4).setCellValue(ckDataCount == 0 ? "No data imported" : "Mismatch");
                            }
                            System.out.println("Sheet finished. Total: " + xlsxDataCount);
                        }
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }
            try (FileOutputStream fos = new FileOutputStream(OUTPUT_FILE)) {
                outputWorkbook.write(fos);
            }
        } catch (SQLException | IOException e) {
            e.printStackTrace();
        }
    }

    // 清洗数字
    private static String cleanNumber(String num) {
        if (num == null || num.isEmpty()) return "0";
        return num.replace(",", "").trim();
    }

    // 获取单元格值
    private static String getCellValue(Cell cell) {
        if (cell == null) return "";
        switch (cell.getCellType()) {
            case STRING:
                return cell.getStringCellValue().trim();
            case NUMERIC:
                if (DateUtil.isCellDateFormatted(cell)) {
                    return new SimpleDateFormat("yyyy/MM/dd").format(cell.getDateCellValue());
                } else {
                    double value = cell.getNumericCellValue();
                    if (value == (long) value) {
                        return String.valueOf((long) value);
                    } else {
                        return String.format("%.2f", value).replaceAll("\\.00$", "");
                    }
                }
            case BOOLEAN:
                return String.valueOf(cell.getBooleanCellValue());
            case FORMULA:
                try {
                    return String.valueOf(cell.getStringCellValue());
                } catch (Exception e) {
                    return String.valueOf(cell.getNumericCellValue());
                }
            default:
                return "";
        }
    }

    private static Connection getConnection() throws SQLException {
        ClickHouseDataSource dataSource = new ClickHouseDataSource(CLICKHOUSE_URL);
        return dataSource.getConnection(CLICKHOUSE_USER, CLICKHOUSE_PASSWORD);
    }

    private static int getClickHouseDataCount(Connection connection, String sheetName) throws SQLException {
        String query = "SELECT COUNT(*) FROM " + CLICKHOUSE_TABLE + " WHERE shop = ?";
        try (PreparedStatement stmt = connection.prepareStatement(query)) {
            stmt.setString(1, sheetName);
            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) return rs.getInt(1);
            }
        }
        return 0;
    }

    private static Workbook getWorkbook(FileInputStream fis, String filePath) throws IOException {
        if (filePath.toLowerCase().endsWith("xlsx")) return new XSSFWorkbook(fis);
        else if (filePath.toLowerCase().endsWith("xls")) return new HSSFWorkbook(fis);
        throw new IllegalArgumentException("Not an Excel file");
    }
}