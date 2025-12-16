package from_csv.shop_700;

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
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
//测试过一次，可以用，且速度更快
public class xlsxtock_2_thread {

    public static void main(String[] args) {
        String directoryPath = "D:\\wzza\\develop\\idea_project\\ceshi\\src\\main\\java\\from_csv\\shop_700\\S3店铺补充-202504";
        String clickhouseUrl = "jdbc:clickhouse://hadoop110:8123/ods?user=default&password=smartpath";
        String tableName = "ods.s3_shop_202504_supplement";
        String reportFilePath = "D:\\wzza\\develop\\idea_project\\ceshi\\src\\main\\java\\from_csv\\shop_700\\report.xlsx";

        ClickHouseDataSource dataSource = new ClickHouseDataSource(clickhouseUrl);

        // 创建报告的工作簿和工作表
        Workbook reportWorkbook = new XSSFWorkbook();
        Sheet reportSheet = reportWorkbook.createSheet("Report");
        Row headerRow = reportSheet.createRow(0);
        headerRow.createCell(0).setCellValue("Shop Name");
        headerRow.createCell(1).setCellValue("Excel Row Count");
        headerRow.createCell(2).setCellValue("CK Row Count");
        headerRow.createCell(3).setCellValue("Match");
        headerRow.createCell(4).setCellValue("Reason");

        // 使用 AtomicInteger 来避免在 lambda 表达式中使用非 final 变量
        AtomicInteger reportRowNum = new AtomicInteger(1);

        // 使用线程池并行处理文件
        ExecutorService executor = Executors.newFixedThreadPool(4);  // 根据文件数调整线程池大小

        try (Connection connection = dataSource.getConnection()) {
            File directory = new File(directoryPath);
            File[] files = directory.listFiles((dir, name) -> name.endsWith(".xlsx"));

            if (files != null) {
                List<Future<Map<String, Object>>> futures = new ArrayList<>();

                // 使用并行流处理文件
                Arrays.stream(files).forEach(file -> {
                    futures.add(executor.submit(() -> processFile(file, connection, tableName, reportSheet, reportRowNum)));
                });

                // 等待所有任务完成并收集报告结果
                for (Future<Map<String, Object>> future : futures) {
                    try {
                        Map<String, Object> result = future.get();
                        String shop = (String) result.get("shop");
                        int excelRowCount = (int) result.get("excelRowCount");
                        int ckRowCount = (int) result.get("ckRowCount");
                        String reason = (String) result.get("reason");

                        // 创建报告行
                        Row reportRow = reportSheet.createRow(reportRowNum.getAndIncrement());
                        reportRow.createCell(0).setCellValue(shop);
                        reportRow.createCell(1).setCellValue(excelRowCount);
                        reportRow.createCell(2).setCellValue(ckRowCount);
                        reportRow.createCell(3).setCellValue(excelRowCount == ckRowCount ? "Yes" : "No");
                        reportRow.createCell(4).setCellValue(reason);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            } else {
                System.out.println("No files found in directory: " + directoryPath);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        // 写入报告文件
        try (FileOutputStream fos = new FileOutputStream(reportFilePath)) {
            reportWorkbook.write(fos);
            System.out.println("Report written to " + reportFilePath);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            executor.shutdown();
        }
    }

    private static Map<String, Object> processFile(File file, Connection connection, String tableName, Sheet reportSheet, AtomicInteger reportRowNum) {
        Map<String, Object> result = new HashMap<>();
        String shop = file.getName().replace(".xlsx", "");
        System.out.println("Processing file: " + file.getName());

        int excelRowCount = 0;
        int ckRowCount = 0;
        String reason = "";

        try (FileInputStream fis = new FileInputStream(file);
             Workbook workbook = new XSSFWorkbook(fis)) {

            Sheet sheet = workbook.getSheetAt(0);
            System.out.println("Processing sheet: " + sheet.getSheetName());

            Iterator<Row> rowIterator = sheet.iterator();

            // Assuming the first row contains column names, so skipping it
            if (rowIterator.hasNext()) rowIterator.next();

            // 创建批量插入的 PreparedStatement
            String sql = "INSERT INTO " + tableName + " (ItemName, URL, ImageURL, Category, Brand, SaleStatus, " +
                    "ReferencePrice, AveragePriceThisMonth, AveragePriceLastMonth, DiscountThisMonth, " +
                    "DiscountLastMonth, SalesThisMonth, SalesLastMonth, SalesAmountThisMonth, " +
                    "SalesAmountLastMonth, UpdateTime, shop, itemid) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
            try (PreparedStatement pstmt = connection.prepareStatement(sql)) {
                while (rowIterator.hasNext()) {
                    Row row = rowIterator.next();
                    excelRowCount++;

                    String itemName = cleanCellValue(row.getCell(0));
                    String url = cleanCellValue(row.getCell(1));
                    String imageURL = cleanCellValue(row.getCell(2));
                    String category = cleanCellValue(row.getCell(3));
                    String brand = cleanCellValue(row.getCell(4));
                    String saleStatus = cleanCellValue(row.getCell(5));
                    String referencePrice = cleanNumericCellValue(row.getCell(6));
                    String averagePriceThisMonth = cleanNumericCellValue(row.getCell(7));
                    String averagePriceLastMonth = cleanNumericCellValue(row.getCell(8));
                    String discountThisMonth = cleanNumericCellValue(row.getCell(9));
                    String discountLastMonth = cleanNumericCellValue(row.getCell(10));
                    String salesThisMonth = cleanNumericCellValue(row.getCell(11));
                    String salesLastMonth = cleanNumericCellValue(row.getCell(12));
                    String salesAmountThisMonth = cleanNumericCellValue(row.getCell(13));
                    String salesAmountLastMonth = cleanNumericCellValue(row.getCell(14));
                    String updateTime = cleanCellValue(row.getCell(15));

                    // Extract numeric item ID from the URL
                    String itemID = url.replaceAll("\\D+", "");

                    pstmt.setString(1, itemName);
                    pstmt.setString(2, url);
                    pstmt.setString(3, imageURL);
                    pstmt.setString(4, category);
                    pstmt.setString(5, brand);
                    pstmt.setString(6, saleStatus);
                    pstmt.setString(7, referencePrice);
                    pstmt.setString(8, averagePriceThisMonth);
                    pstmt.setString(9, averagePriceLastMonth);
                    pstmt.setString(10, discountThisMonth);
                    pstmt.setString(11, discountLastMonth);
                    pstmt.setString(12, salesThisMonth);
                    pstmt.setString(13, salesLastMonth);
                    pstmt.setString(14, salesAmountThisMonth);
                    pstmt.setString(15, salesAmountLastMonth);
                    pstmt.setString(16, updateTime);
                    pstmt.setString(17, shop);
                    pstmt.setString(18, itemID);

                    pstmt.addBatch();  // Add batch instead of executing after each row
                }

                pstmt.executeBatch();  // Execute batch
                System.out.println("Inserted rows for shop: " + shop);

            } catch (Exception e) {
                reason += "Error inserting data for shop: " + shop + ", error: " + e.getMessage() + "; ";
                e.printStackTrace();
            }

            // 查询ClickHouse中该shop的数据量
            String countSql = "SELECT COUNT(*) FROM " + tableName + " WHERE shop = ?";
            try (PreparedStatement pstmt = connection.prepareStatement(countSql)) {
                pstmt.setString(1, shop);
                ResultSet rs = pstmt.executeQuery();
                if (rs.next()) {
                    ckRowCount = rs.getInt(1);
                }
            }

        } catch (Exception e) {
            reason += "Error processing file: " + file.getName() + ", error: " + e.getMessage();
            e.printStackTrace();
        }

        result.put("shop", shop);
        result.put("excelRowCount", excelRowCount);
        result.put("ckRowCount", ckRowCount);
        result.put("reason", reason);

        return result;
    }

    private static String cleanCellValue(Cell cell) {
        if (cell == null) {
            return "";
        }
        String value = cell.toString();
        return value.replaceAll("[\n\r\t]", "").trim();
    }

    private static String cleanNumericCellValue(Cell cell) {
        if (cell == null) {
            return "";
        }
        if (cell.getCellType() == CellType.NUMERIC) {
            double numericValue = cell.getNumericCellValue();
            if (numericValue == (long) numericValue) {
                return String.valueOf((long) numericValue);
            } else {
                return String.valueOf(numericValue);
            }
        } else {
            return cleanCellValue(cell);
        }
    }
}
