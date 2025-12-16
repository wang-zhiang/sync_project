package mysqlutil;




import java.io.FileOutputStream;
        import java.sql.Connection;
        import java.sql.DriverManager;
        import java.sql.ResultSet;
        import java.sql.Statement;
        import org.apache.poi.ss.usermodel.*;
        import org.apache.poi.xssf.usermodel.XSSFWorkbook;

public class get_count {
    public static void main(String[] args) {
        try (Workbook workbook = new XSSFWorkbook();
             FileOutputStream fileOut = new FileOutputStream("count.xlsx")) {

            String[] data = {
                    "202103 elmmarket mysql Server=192.168.4.34;Database=elm2;Uid=root;Pwd=smartpthdata;maximumpoolsize=500;Charset=utf8mb4;",

            };

            for (String entry : data) {
                String[] parts = entry.split(" ");
                String month = parts[0];
                String tableName = parts[1];
                String dbInfo = parts[2];
                String query = "SELECT COUNT(*) AS count, SUM(current_price) AS total_price FROM " + tableName + "." + tableName + "_" + month;

                try (Connection connection = DriverManager.getConnection("jdbc:" + dbInfo);
                     Statement statement = connection.createStatement();
                     ResultSet resultSet = statement.executeQuery(query)) {

                    Sheet sheet = workbook.createSheet(month + "_" + tableName);
                    int rowNum = 0;
                    while (resultSet.next()) {
                        Row row = sheet.createRow(rowNum++);
                        row.createCell(0).setCellValue(month);
                        row.createCell(1).setCellValue(tableName);
                        row.createCell(2).setCellValue(resultSet.getInt("count"));
                        row.createCell(3).setCellValue(resultSet.getDouble("total_price"));
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            workbook.write(fileOut);
            System.out.println("Excel file has been generated successfully!");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
