package from_csv.csv_ck;

import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.ss.usermodel.*;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


//目前用这个，因为有三行的还有一行的，这个都能兼容
public class douyin_duohang_sqlserver_new2 {
    private static final String INPUT_FOLDER = "D:\\wzza\\develop\\idea_project\\ceshi\\src\\main\\java\\from_csv\\csv_ck\\抖音";

    // SQL Server 配置
    //private static final String DB_URL = "jdbc:sqlserver://192.168.3.183;DatabaseName=sourcedate;encrypt=false;trustServerCertificate=true";
    private static final String DB_URL = "jdbc:sqlserver://192.168.4.57;DatabaseName=TradingDouYin;encrypt=false;trustServerCertificate=true";
    private static final String DB_USER = "sa";
    private static final String DB_PASSWORD = "smartpthdata";
    private static final String TABLE_NAME = "dy_s3_shop_202601_test";

    private static final String OUTPUT_FILE = "D:\\wzza\\develop\\idea_project\\ceshi\\src\\main\\java\\from_csv\\csv_ck\\data_verification.xlsx";

    public static void main(String[] args) {
        // 正则：匹配 id=xxx 或 goods_id=xxx
        Pattern patternId = Pattern.compile("(?:id|goods_id)=(\\d+)");

        try (Connection connection = getConnection()) {
            createTableIfNotExist(connection);

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
                headerRow.createCell(2).setCellValue("DB Data Count");
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

                            if (sheet == null || sheet.getLastRowNum() < 1) {
                                System.out.println("Skipping empty sheet: " + sheetName);
                                continue;
                            }

                            // --- 动态检测格式 (1行模式 vs 3行模式) ---
                            int step = 3;
                            Row firstDataRow = sheet.getRow(1);
                            String firstColA = "";
                            if (firstDataRow != null) {
                                firstColA = getCellValue(firstDataRow.getCell(0));
                            }

                            // 如果第一行A列有字，且不是"0.0"这种错误数据，说明是1行模式
                            if (!firstColA.isEmpty() && !firstColA.equals("0.0")) {
                                step = 1;
                                System.out.println("   -> 检测到格式：【单行模式】(Step=1)");
                            } else {
                                step = 3;
                                System.out.println("   -> 检测到格式：【三行模式】(Step=3)");
                            }
                            // ------------------------------------

                            String sql = "INSERT INTO " + TABLE_NAME + " (ItemName, ProductCategory, SalesStatus, Brand, ReferencePrice, AvgTransactionPrice, PostReturnSalesVolume, PostReturnSalesAmount, itemid, shop) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
                            int xlsxDataCount = 0;

                            try (PreparedStatement statement = connection.prepareStatement(sql)) {
                                int processedRows = 0;

                                for (int i = 1; i <= sheet.getLastRowNum(); i += step) {
                                    Row rowTop = sheet.getRow(i);
                                    Row rowName = (step == 1) ? rowTop : sheet.getRow(i + 2);

                                    if (rowTop == null && rowName == null) continue;

                                    // ================== 修改开始 ==================

                                    // 1. 获取 A 列的内容（商品名称候选）
                                    String valCheck = "";
                                    if (rowName != null) {
                                        valCheck = getCellValue(rowName.getCell(0));
                                    } else if (rowTop != null) {
                                        // 防御性代码，如果rowName为空，尝试看rowTop
                                        valCheck = getCellValue(rowTop.getCell(0));
                                    }

                                    // 2. 执行过滤逻辑
                                    // 2.1 空值检查
                                    if (valCheck == null || valCheck.trim().isEmpty()) continue;

                                    // 2.2 关键词过滤：过滤 "合计"、"条/页"、"共xx条"
                                    if (valCheck.contains("本页合计") ||
                                            valCheck.contains("全部合计") ||
                                            valCheck.contains("条/页")) {
                                        continue;
                                    }

                                    // 2.3 纯数字过滤：使用正则匹配纯数字 (例如 "2", "3", "100")
                                    // ^\d+$ 表示从头到尾只有数字
                                    if (valCheck.matches("^\\d+$")) {
                                        System.out.println("   -> 跳过页码行/纯数字行: " + valCheck);
                                        continue;
                                    }

                                     // 2.4 (可选) 长度过滤：商品名称通常较长，如果长度小于2且不是特殊符号，可能也是垃圾数据
                                    if (valCheck.length() < 2) {
                                        System.out.println("   -> 跳过过短数据: " + valCheck);
                                        continue;
                                    }

                                    // --- 1. 获取 ItemName & ID ---
                                    String itemName = "";
                                    String itemid = "";

                                    if (rowName != null) {
                                        Cell nameCell = rowName.getCell(0);
                                        // 使用修复后的 getCellValue 获取名字
                                        itemName = getCellValue(nameCell);

                                        // 如果仍然是 0.0，强制设为空字符串，避免脏数据
                                        if ("0.0".equals(itemName)) itemName = "";

                                        if (nameCell != null) {
                                            String url = getLinkFromCell(nameCell);
                                            if (url != null && !url.isEmpty()) {
                                                Matcher m = patternId.matcher(url);
                                                if (m.find()) {
                                                    itemid = m.group(1);
                                                }
                                            }
                                        }
                                    }

                                    if (itemName.isEmpty()) continue;

                                    // --- 2. 获取其他列 ---
                                    String productCategory = "";
                                    String salesStatus = "";
                                    String brand = "";
                                    String referencePrice = "";
                                    String avgTransactionPrice = "";
                                    String postReturnSalesVolume = "";
                                    String postReturnSalesAmount = "";

                                    if (rowTop != null) {
                                        productCategory = getCellValue(rowTop.getCell(1));
                                        salesStatus = getCellValue(rowTop.getCell(2));
                                        brand = getCellValue(rowTop.getCell(3));
                                        referencePrice = getCellValue(rowTop.getCell(4));
                                        avgTransactionPrice = getCellValue(rowTop.getCell(5));
                                        postReturnSalesVolume = getCellValue(rowTop.getCell(6));
                                        postReturnSalesAmount = getCellValue(rowTop.getCell(7));
                                    }

                                    if (processedRows == 0) {
                                        System.out.println("✅ Debug Record (Row " + i + "):");
                                        System.out.println("   Name: " + itemName);
                                        System.out.println("   ItemID: " + itemid);
                                    }

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

                            outputRow.createCell(1).setCellValue(xlsxDataCount);
                            int dbDataCount = getDbDataCount(connection, sheetName);
                            outputRow.createCell(2).setCellValue(dbDataCount);
                            boolean isCountEqual = xlsxDataCount == dbDataCount && dbDataCount > 0;
                            outputRow.createCell(3).setCellValue(isCountEqual);
                            if (!isCountEqual) {
                                outputRow.createCell(4).setCellValue(dbDataCount == 0 ? "No data imported" : "Mismatch");
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

    // --- 重点修改：修复了 "0.0" 问题的 getCellValue ---
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
                    // 1. 尝试直接获取字符串结果
                    return String.valueOf(cell.getStringCellValue());
                } catch (Exception e) {
                    // 2. 如果报错（说明没有缓存结果），尝试解析公式字符串
                    try {
                        String formula = cell.getCellFormula();
                        // 处理 HYPERLINK 公式： HYPERLINK("url", "friendly_name")
                        if (formula != null && formula.contains("HYPERLINK")) {
                            // 简单的解析逻辑：获取最后一个逗号之后的内容，并去掉引号和括号
                            int commaIndex = formula.lastIndexOf(",");
                            if (commaIndex > -1) {
                                String namePart = formula.substring(commaIndex + 1);
                                // 去掉结束括号、双引号、单引号
                                return namePart.replaceAll("[\\)\"']", "").trim();
                            }
                        }
                    } catch (Exception ex) {
                        // 忽略公式解析错误
                    }

                    // 3. 最后才尝试数字，如果数字是0，返回空字符串而不是 "0.0"
                    try {
                        double val = cell.getNumericCellValue();
                        if (val == 0) return ""; // 避免返回 "0.0"
                        return String.valueOf(val);
                    } catch (Exception ex) {
                        return "";
                    }
                }
            default:
                return "";
        }
    }

    private static String getLinkFromCell(Cell cell) {
        if (cell == null) return null;
        Hyperlink link = cell.getHyperlink();
        if (link != null) {
            return link.getAddress();
        }
        if (cell.getCellType() == CellType.FORMULA) {
            try {
                String formula = cell.getCellFormula();
                if (formula.contains("HYPERLINK")) {
                    int firstQuote = formula.indexOf("\"");
                    int secondQuote = formula.indexOf("\"", firstQuote + 1);
                    if (firstQuote != -1 && secondQuote != -1) {
                        return formula.substring(firstQuote + 1, secondQuote);
                    }
                }
            } catch (Exception e) { }
        }
        return null;
    }

    private static void createTableIfNotExist(Connection conn) throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            String checkSchemaSql = "IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'ods') BEGIN EXEC('CREATE SCHEMA [ods]') END";
            stmt.execute(checkSchemaSql);

            String createTableSql = "IF OBJECT_ID('" + TABLE_NAME + "', 'U') IS NULL " +
                    "BEGIN " +
                    "CREATE TABLE " + TABLE_NAME + " (" +
                    "ItemName NVARCHAR(MAX), " +
                    "ProductCategory NVARCHAR(MAX), " +
                    "SalesStatus NVARCHAR(MAX), " +
                    "Brand NVARCHAR(MAX), " +
                    "ReferencePrice NVARCHAR(MAX), " +
                    "AvgTransactionPrice NVARCHAR(MAX), " +
                    "PostReturnSalesAmount NVARCHAR(MAX), " +
                    "PostReturnSalesVolume NVARCHAR(MAX), " +
                    "itemid NVARCHAR(MAX), " +
                    "shop NVARCHAR(MAX) " +
                    ") " +
                    "END";

            stmt.execute(createTableSql);
        }
    }

    private static String cleanNumber(String num) {
        if (num == null || num.isEmpty()) return "0";
        return num.replace(",", "").trim();
    }

    private static Connection getConnection() throws SQLException {
        try {
            Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            throw new SQLException("SQL Server Driver not found");
        }
        return DriverManager.getConnection(DB_URL, DB_USER, DB_PASSWORD);
    }

    private static int getDbDataCount(Connection connection, String sheetName) throws SQLException {
        String query = "SELECT COUNT(*) FROM " + TABLE_NAME + " WHERE shop = ?";
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