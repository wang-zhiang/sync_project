package csv_temp.pet;



import org.apache.poi.ss.usermodel.*;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;

public class ExcelToSqlServer {

    // ================= 配置区域 =================
    // 请在这里替换你的文件夹路径
    private static final String FOLDER_PATH = "D:\\wzza\\develop\\idea_project\\ceshi\\src\\main\\java\\csv_temp\\pet\\69家90";

    // 数据库配置
    // 注意：你给的信息里有 "3.72" 和 "websearchc"。
    // 如果 "websearchc" 是数据库名字，请保留。如果不是，请修改 databaseName=你的数据库名
    // 请将 "YOUR_IP_HERE" 替换为实际的 IP 地址 (例如 192.168.1.100 或 localhost)
    private static final String DB_URL = "jdbc:sqlserver://192.168.3.72;databaseName=websearchc;";
    private static final String DB_USER = "sa";
    private static final String DB_PASSWORD = "smartpathdata";

    // 表名
    private static final String TABLE_NAME = "PetDataImport";
    // ===========================================

    public static void main(String[] args) {
        try {
            // 1. 连接数据库
            Connection conn = DriverManager.getConnection(DB_URL, DB_USER, DB_PASSWORD);
            System.out.println("数据库连接成功！");

            // 2. 创建表（如果不存在）
            createTableIfNotExists(conn);

            // 3. 遍历文件夹并处理文件
            File folder = new File(FOLDER_PATH);
            if (!folder.exists() || !folder.isDirectory()) {
                System.err.println("文件夹不存在或路径错误: " + FOLDER_PATH);
                return;
            }

            File[] files = folder.listFiles((dir, name) -> name.toLowerCase().endsWith(".xlsx") || name.toLowerCase().endsWith(".xls"));
            if (files == null || files.length == 0) {
                System.out.println("文件夹为空或没有Excel文件。");
                return;
            }

            for (File file : files) {
                System.out.println("正在处理文件: " + file.getName());
                processExcelFile(conn, file);
            }

            System.out.println("所有文件处理完成！");
            conn.close();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 创建 SQL Server 表，所有字段使用 nvarchar(max)
     */
    private static void createTableIfNotExists(Connection conn) throws SQLException {
        String sql = "IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='" + TABLE_NAME + "' AND xtype='U') " +
                "BEGIN " +
                "CREATE TABLE " + TABLE_NAME + " (" +
                "   [id] NVARCHAR(MAX), " +
                "   [sku] NVARCHAR(MAX), " +
                "   [title] NVARCHAR(MAX), " + // 标题
                "   [vol_15_days] NVARCHAR(MAX), " + // 15日计入单量
                "   [est_amount] NVARCHAR(MAX), " + // 预估金额
                "   [text_weight] NVARCHAR(MAX), " + // 文本权重
                "   [price] NVARCHAR(MAX), " + // 价格
                "   [sold_review_count] NVARCHAR(MAX), " + // 售出/评价数
                "   [positive_rate_uv] NVARCHAR(MAX), " + // 好评率/UV
                "   [shop_name] NVARCHAR(MAX), " + // 店铺名称
                "   [query_time] NVARCHAR(MAX), " + // 查询时间
                "   [file_name] NVARCHAR(MAX)" + // **额外添加的文件名字段**
                ") " +
                "END";

        try (Statement stmt = conn.createStatement()) {
            stmt.execute(sql);
            System.out.println("表结构检查完毕（如果不存在已自动创建）。");
        }
    }

    /**
     * 读取单个 Excel 文件并插入数据库
     */
    private static void processExcelFile(Connection conn, File file) {
        // SQL 插入语句
        String insertSql = "INSERT INTO " + TABLE_NAME + " " +
                "([id], [sku], [title], [vol_15_days], [est_amount], [text_weight], " +
                "[price], [sold_review_count], [positive_rate_uv], [shop_name], [query_time], [file_name]) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

        try (FileInputStream fis = new FileInputStream(file);
             Workbook workbook = new XSSFWorkbook(fis); // 这里的 Workbook 可以自动处理 .xlsx
             PreparedStatement pstmt = conn.prepareStatement(insertSql)) {

            Sheet sheet = workbook.getSheetAt(0); // 读取第一个 Sheet
            int rowCount = 0;

            // 遍历每一行
            for (Row row : sheet) {
                // 跳过第一行（通常是表头），如果你的Excel没有表头，请把 if 去掉
                if (row.getRowNum() == 0) {
                    continue;
                }

                // 假设 Excel 的列顺序和需求一致，从第0列开始
                // 1. id
                pstmt.setString(1, getCellValueAsString(row.getCell(0)));
                // 2. sku
                pstmt.setString(2, getCellValueAsString(row.getCell(1)));
                // 3. 标题
                pstmt.setString(3, getCellValueAsString(row.getCell(2)));
                // 4. 15日计入单量
                pstmt.setString(4, getCellValueAsString(row.getCell(3)));
                // 5. 预估金额
                pstmt.setString(5, getCellValueAsString(row.getCell(4)));
                // 6. 文本权重
                pstmt.setString(6, getCellValueAsString(row.getCell(5)));
                // 7. 价格
                pstmt.setString(7, getCellValueAsString(row.getCell(6)));
                // 8. 售出/评价数
                pstmt.setString(8, getCellValueAsString(row.getCell(7)));
                // 9. 好评率/UV
                pstmt.setString(9, getCellValueAsString(row.getCell(8)));
                // 10. 店铺名称
                pstmt.setString(10, getCellValueAsString(row.getCell(9)));
                // 11. 查询时间
                pstmt.setString(11, getCellValueAsString(row.getCell(10)));

                // 12. **文件名** (这是额外追加的字段)
                pstmt.setString(12, file.getName());

                // 添加到批处理
                pstmt.addBatch();
                rowCount++;

                // 每1000行执行一次插入，防止内存溢出
                if (rowCount % 1000 == 0) {
                    pstmt.executeBatch();
                    pstmt.clearBatch();
                }
            }

            // 执行剩余的批处理
            pstmt.executeBatch();
            System.out.println("文件 " + file.getName() + " 导入成功，共导入 " + rowCount + " 行数据。");

        } catch (IOException | SQLException e) {
            System.err.println("处理文件 " + file.getName() + " 时出错: " + e.getMessage());
            e.printStackTrace();
        }
    }

    /**
     * 辅助方法：不管单元格是什么类型（数字、文本、公式），都转成 String
     */
    private static String getCellValueAsString(Cell cell) {
        if (cell == null) {
            return "";
        }
        try {
            switch (cell.getCellType()) {
                case STRING:
                    return cell.getStringCellValue();
                case NUMERIC:
                    // 如果是日期
                    if (DateUtil.isCellDateFormatted(cell)) {
                        return cell.getLocalDateTimeCellValue().toString();
                    }
                    // 避免科学计数法，转为普通字符串
                    double val = cell.getNumericCellValue();
                    if (val == (long) val) {
                        return String.format("%d", (long) val);
                    } else {
                        return String.format("%s", val);
                    }
                case BOOLEAN:
                    return String.valueOf(cell.getBooleanCellValue());
                case FORMULA:
                    try {
                        return String.valueOf(cell.getNumericCellValue());
                    } catch (IllegalStateException e) {
                        return cell.getStringCellValue();
                    }
                case BLANK:
                    return "";
                default:
                    return "";
            }
        } catch (Exception e) {
            return "";
        }
    }
}