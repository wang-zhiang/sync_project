package from_csv.shop_700;

import org.apache.poi.ss.usermodel.*;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import ru.yandex.clickhouse.ClickHouseDataSource;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.Iterator;

public class XLSXToClickHouse {

    public static void main(String[] args) {
        String directoryPath = "D:\\wzza\\develop\\idea_project\\ceshi\\src\\main\\java\\from_csv\\shop_700\\S3-账号2数据下载-202407.V2";
        String clickhouseUrl = "jdbc:clickhouse://hadoop110:8123/ods?user=default&password=smartpath";
        String tableName = "ods.s3_shop_2407_v2";

        ClickHouseDataSource dataSource = new ClickHouseDataSource(clickhouseUrl);

        try (Connection connection = dataSource.getConnection()) {
            File directory = new File(directoryPath);
            File[] files = directory.listFiles((dir, name) -> name.endsWith(".xlsx"));

            if (files != null) {
                for (File file : files) {
                    String shop = file.getName().replace(".xlsx", "");
                    System.out.println("Processing file: " + file.getName());
                    try (FileInputStream fis = new FileInputStream(file);
                         Workbook workbook = new XSSFWorkbook(fis)) {

                        Sheet sheet = workbook.getSheetAt(0);
                        System.out.println("Processing sheet: " + sheet.getSheetName());

                        Iterator<Row> rowIterator = sheet.iterator();

                        // Assuming the first row contains column names, so skipping it
                        if (rowIterator.hasNext()) rowIterator.next();

                        while (rowIterator.hasNext()) {
                            Row row = rowIterator.next();

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

                            String sql = "INSERT INTO " + tableName + " (ItemName, URL, ImageURL, Category, Brand, SaleStatus, " +
                                    "ReferencePrice, AveragePriceThisMonth, AveragePriceLastMonth, DiscountThisMonth, " +
                                    "DiscountLastMonth, SalesThisMonth, SalesLastMonth, SalesAmountThisMonth, " +
                                    "SalesAmountLastMonth, UpdateTime, shop, itemid) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

                            try (PreparedStatement pstmt = connection.prepareStatement(sql)) {
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

                                pstmt.executeUpdate();
                                System.out.println("Inserted row for item: " + itemName);
                            } catch (Exception e) {
                                System.err.println("Error inserting row for item: " + itemName + ", error: " + e.getMessage());
                                e.printStackTrace();
                            }
                        }
                    } catch (Exception e) {
                        System.err.println("Error processing file: " + file.getName() + ", error: " + e.getMessage());
                        e.printStackTrace();
                    }
                }
            } else {
                System.out.println("No files found in directory: " + directoryPath);
            }
        } catch (Exception e) {
            System.err.println("Error connecting to ClickHouse, error: " + e.getMessage());
            e.printStackTrace();
        }
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
