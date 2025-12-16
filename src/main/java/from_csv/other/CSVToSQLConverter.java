package from_csv.other;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;

public class CSVToSQLConverter {
    public static void main(String[] args) {
        // Set variables
        String industryId = "34";
        String industrySubId = "89";
        String serNum = "100"; // Predefined variable

        String inputCSV = "D:\\wzza\\develop\\idea_project\\ceshi\\src\\main\\java\\from_csv\\other\\ceshi.csv";
        String outputTXT = "D:\\wzza\\develop\\idea_project\\ceshi\\src\\main\\java\\from_csv\\other\\output.txt";

        try (BufferedWriter writer = Files.newBufferedWriter(Paths.get(outputTXT), StandardCharsets.UTF_8)) {
            List<String> lines = Files.readAllLines(Paths.get(inputCSV), StandardCharsets.UTF_8);

            // Skip the header line
            for (int i = 1; i < lines.size(); i++) {
                String[] values = lines.get(i).split(",");
                String targetMonth = formatYearMonth(values[0]); // Column: targetMonth
                String sourceMonth = formatYearMonth(values[1]); // Column: sourceMonth
                String coefficient = values[2]; // Column: coefficient

                String sql = generateSQL(targetMonth, sourceMonth, coefficient, industryId, industrySubId, serNum);
                writer.write(sql);
                writer.newLine();
            }

            System.out.println("SQL statements generated and written to " + outputTXT);

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    // Method to format date as yyyyMM
    private static String formatYearMonth(String date) {
        String[] parts = date.split("/");
        String year = parts[0];
        String month = parts[1].length() == 1 ? "0" + parts[1] : parts[1]; // Ensure month is two digits
        return year + month;
    }

    private static String generateSQL(String targetMonth, String sourceMonth, String coefficient, String industryId, String industrySubId, String serNum) {
        return "INSERT INTO ods.elmoriginaldetail (\n" +
                "    IndustryName,\n" +
                "    IndustrySubName,\n" +
                "    CategoryName,\n" +
                "    BrandName,\n" +
                "    Channel,\n" +
                "    IndustryId,\n" +
                "    IndustrySubId,\n" +
                "    BrandId,\n" +
                "    CategoryId,\n" +
                "    SeriesId,\n" +
                "    skuid,\n" +
                "    SeriesName,\n" +
                "    SkuName,\n" +
                "    id,\n" +
                "    shopid,\n" +
                "    itemid,\n" +
                "    ItemName,\n" +
                "    qty,\n" +
                "    shopname,\n" +
                "    price,\n" +
                "    pricepromotion,\n" +
                "    rate,\n" +
                "    updatetime,\n" +
                "    categoryfirst,\n" +
                "    categorysecond,\n" +
                "    sku_sort,\n" +
                "    img_url,\n" +
                "    stock_count,\n" +
                "    upc,\n" +
                "    drug_info,\n" +
                "    corner_mark,\n" +
                "    province,\n" +
                "    city,\n" +
                "    drict,\n" +
                "    classify,\n" +
                "    sell_out,\n" +
                "    discount,\n" +
                "    elm_sku_id,\n" +
                "    cornerMark,\n" +
                "    pt_ym,\n" +
                "    ser_num\n" +
                ")\n" +
                "SELECT\n" +
                "    IndustryName,\n" +
                "    IndustrySubName,\n" +
                "    CategoryName,\n" +
                "    BrandName,\n" +
                "    Channel,\n" +
                "    IndustryId,\n" +
                "    IndustrySubId,\n" +
                "    BrandId,\n" +
                "    CategoryId,\n" +
                "    SeriesId,\n" +
                "    skuid,\n" +
                "    SeriesName,\n" +
                "    SkuName,\n" +
                "    id,\n" +
                "    shopid,\n" +
                "    itemid,\n" +
                "    ItemName,\n" +
                "    qty * " + coefficient + ",\n" + // Multiply qty by the coefficient from CSV
                "    shopname,\n" +
                "    price,\n" +
                "    pricepromotion,\n" +
                "    rate,\n" +
                "    updatetime,\n" +
                "    categoryfirst,\n" +
                "    categorysecond,\n" +
                "    sku_sort,\n" +
                "    img_url,\n" +
                "    stock_count,\n" +
                "    upc,\n" +
                "    drug_info,\n" +
                "    corner_mark,\n" +
                "    province,\n" +
                "    city,\n" +
                "    drict,\n" +
                "    classify,\n" +
                "    sell_out,\n" +
                "    discount,\n" +
                "    elm_sku_id,\n" +
                "    cornerMark,\n" +
                "    '" + targetMonth + "',\n" + // Use formatted target month from CSV
                "    '" + serNum + "'\n" +
                "FROM ods.elmoriginaldetail\n" +
                "WHERE IndustryId = '" + industryId + "'\n" +
                "  AND IndustrySubId = '" + industrySubId + "'\n" +
                "  AND pt_ym = '" + sourceMonth + "'\n" + // Use formatted source month from CSV
                "  AND itemid GLOBAL NOT IN (\n" +
                "      SELECT DISTINCT itemid\n" +
                "      FROM ods.elmoriginaldetail\n" +
                "      WHERE IndustryId = '" + industryId + "'\n" +
                "        AND IndustrySubId = '" + industrySubId + "'\n" +
                "        AND pt_ym = '" + targetMonth + "'\n" +
                "  );";
    }
}
