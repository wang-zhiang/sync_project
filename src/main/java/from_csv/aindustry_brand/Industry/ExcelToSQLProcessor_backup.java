package from_csv.aindustry_brand.Industry;

import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.ss.usermodel.*;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class ExcelToSQLProcessor_backup {
    
    // 数据库连接信息
    private static final String DB_SERVER = "192.168.4.51";
    private static final String DB_NAME = "taobao_trading";
    private static final String DB_USER = "sa";
    private static final String DB_PASSWORD = "smartpathdata";
    private static final String DB_URL = "jdbc:sqlserver://" + DB_SERVER + ":1433;databaseName=" + DB_NAME + ";encrypt=false;trustServerCertificate=true;";
    
    // 表名变量，用户可以修改
    private static final String SALES_TABLE_NAME = "Industry_1_month_1"; // 用户填入销售额表名
    private static final String VOLUME_TABLE_NAME = "Industry_1_month_2"; // 用户填入销量表名
    
    // 数据库字段定义
    private static final String[] FIELDS = {
        "一级类目", "二级类目", "三级类目",
        "所选类目整体", "天猫", "淘宝", "京东", "抖音", "得物", "小红书", "拼多多",
        "所选类目整体同比", "天猫同比", "淘宝同比", "京东同比", "抖音同比", "得物同比", "小红书同比", "拼多多同比",
        "所选类目整体同比_百分比", "天猫同比_百分比", "淘宝同比_百分比", "京东同比_百分比", 
        "抖音同比_百分比", "得物同比_百分比", "小红书同比_百分比", "拼多多同比_百分比"
    };
    
    private static Connection connection;
    
    public static void main(String[] args) {
        try {
            // 连接数据库
            connectToDatabase();
            
            // 处理文件夹中的所有Excel文件
            String basePath = "D:\\wzza\\develop\\idea_project\\ceshi\\src\\main\\java\\from_csv\\aindustry_brand\\2025-1行业洞察"; // 根据实际情况调整路径
            System.out.println("开始处理路径: " + basePath);
            processExcelFiles(basePath);
            
            System.out.println("处理完成！数据已插入到数据库。");
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            // 关闭数据库连接
            closeDatabase();
        }
    }
    
    /**
     * 连接到数据库
     */
    private static void connectToDatabase() throws SQLException {
        try {
            Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
            connection = DriverManager.getConnection(DB_URL, DB_USER, DB_PASSWORD);
            connection.setAutoCommit(false); // 使用事务
            System.out.println("数据库连接成功！");
        } catch (ClassNotFoundException e) {
            throw new SQLException("找不到SQLServer驱动程序", e);
        }
    }
    
    /**
     * 关闭数据库连接
     */
    private static void closeDatabase() {
        if (connection != null) {
            try {
                connection.close();
                System.out.println("数据库连接已关闭。");
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }
    
    /**
     * 处理指定路径下的所有Excel文件
     */
    public static void processExcelFiles(String basePath) throws IOException, SQLException {
        File baseDir = new File(basePath);
        
        if (!baseDir.exists()) {
            System.out.println("路径不存在: " + baseDir.getAbsolutePath());
            return;
        }
        
        System.out.println("处理目录: " + baseDir.getAbsolutePath());
        
        // 检查是否有大文件夹结构
        File[] allFiles = baseDir.listFiles();
        boolean hasSubDirs = false;
        
        if (allFiles != null) {
            System.out.println("找到 " + allFiles.length + " 个文件/文件夹");
            for (File file : allFiles) {
                System.out.println("处理: " + file.getName() + " (是目录: " + file.isDirectory() + ")");
                if (file.isDirectory()) {
                    hasSubDirs = true;
                    // 直接把子文件夹当作一级分类处理
                    String firstCategory = file.getName();
                    System.out.println("找到一级分类: " + firstCategory);
                    processFirstCategoryDirectory(file, firstCategory);
                }
            }
        } else {
            System.out.println("目录为空或无法读取");
        }
        
        // 如果没有子文件夹，直接处理当前目录下的Excel文件
        if (!hasSubDirs) {
            File[] excelFiles = baseDir.listFiles((dir, name) -> 
                name.toLowerCase().endsWith(".xlsx") || name.toLowerCase().endsWith(".xls"));
            if (excelFiles != null) {
                for (File excelFile : excelFiles) {
                    String fileName = excelFile.getName();
                    String secondCategory = fileName.substring(0, fileName.lastIndexOf('.'));
                    processExcelFile(excelFile, "默认一级类目", secondCategory);
                }
            }
        }
        
        System.out.println("所有文件处理完成！");
    }
    

    
    /**
     * 处理一级分类目录（包含Excel文件）
     */
    private static void processFirstCategoryDirectory(File firstCategoryDir, String firstCategory) throws IOException, SQLException {
        System.out.println("处理一级分类目录: " + firstCategoryDir.getName());
        
        // 遍历一级分类目录下的Excel文件
        File[] excelFiles = firstCategoryDir.listFiles((dir, name) -> 
            name.toLowerCase().endsWith(".xlsx") || name.toLowerCase().endsWith(".xls"));
        
        if (excelFiles != null && excelFiles.length > 0) {
            System.out.println("找到 " + excelFiles.length + " 个Excel文件");
            for (File excelFile : excelFiles) {
                // Excel文件名作为二级分类
                String fileName = excelFile.getName();
                String secondCategory = fileName.substring(0, fileName.lastIndexOf('.'));
                System.out.println("处理Excel文件: " + fileName + " -> 二级分类: " + secondCategory);
                processExcelFile(excelFile, firstCategory, secondCategory);
            }
        } else {
            System.out.println("在目录 " + firstCategoryDir.getName() + " 下没有找到Excel文件");
        }
    }
    
    /**
     * 处理单个Excel文件
     */
    private static void processExcelFile(File excelFile, String firstCategory, String secondCategory) throws IOException, SQLException {
        
        System.out.println("正在处理文件: " + excelFile.getAbsolutePath());
        
        try (FileInputStream fis = new FileInputStream(excelFile)) {
            Workbook workbook;
            
            // 根据文件扩展名选择合适的工作簿类型
            if (excelFile.getName().toLowerCase().endsWith(".xlsx")) {
                workbook = new XSSFWorkbook(fis);
            } else {
                workbook = new HSSFWorkbook(fis);
            }
            
            // 处理第一个工作表
            Sheet sheet = workbook.getSheetAt(0);
            
            // 解析数据并插入数据库
            List<String[]> salesData = new ArrayList<>();
            List<String[]> volumeData = new ArrayList<>();
            
            parseExcelData(sheet, firstCategory, secondCategory, salesData, volumeData);
            
            System.out.println("解析完成 - 销售额数据: " + salesData.size() + " 条, 销量数据: " + volumeData.size() + " 条");
            
            // 插入数据到数据库
            if (!salesData.isEmpty()) {
                System.out.println("开始插入销售额数据...");
                insertDataToDatabase(salesData, SALES_TABLE_NAME);
            } else {
                System.out.println("销售额数据为空，跳过插入");
            }
            
            if (!volumeData.isEmpty()) {
                System.out.println("开始插入销量数据...");
                insertDataToDatabase(volumeData, VOLUME_TABLE_NAME);
            } else {
                System.out.println("销量数据为空，跳过插入");
            }
            
            workbook.close();
            
        } catch (Exception e) {
            System.err.println("处理文件失败: " + excelFile.getAbsolutePath() + " - " + e.getMessage());
            e.printStackTrace();
            // 回滚事务
            connection.rollback();
            throw e;
        }
    }
    
    /**
     * 插入数据到数据库
     */
    private static void insertDataToDatabase(List<String[]> data, String tableName) throws SQLException {
        if (data.isEmpty()) {
            System.out.println("没有数据需要插入到表: " + tableName);
            return;
        }
        
        // 构建INSERT语句
        StringBuilder sql = new StringBuilder();
        sql.append("INSERT INTO [").append(tableName).append("] (");
        
        // 添加字段名
        for (int i = 0; i < FIELDS.length; i++) {
            if (i > 0) sql.append(", ");
            sql.append("[").append(FIELDS[i]).append("]");
        }
        
        sql.append(") VALUES (");
        
        // 添加参数占位符
        for (int i = 0; i < FIELDS.length; i++) {
            if (i > 0) sql.append(", ");
            sql.append("?");
        }
        
        sql.append(")");
        
        System.out.println("执行SQL: " + sql.toString());
        
        try (PreparedStatement pstmt = connection.prepareStatement(sql.toString())) {
            int batchCount = 0;
            int[] results;
            
            for (String[] record : data) {
                // 设置参数值
                for (int i = 0; i < record.length; i++) {
                    String value = record[i] != null ? record[i] : "";
                    pstmt.setString(i + 1, value);
                }
                
                pstmt.addBatch();
                batchCount++;
                
                // 每1000条执行一次批处理
                if (batchCount % 1000 == 0) {
                    results = pstmt.executeBatch();
                    System.out.println("批处理执行结果: " + java.util.Arrays.toString(results));
                    System.out.println("已插入 " + batchCount + " 条数据到表: " + tableName);
                }
            }
            
            // 执行剩余的批处理
            if (batchCount % 1000 != 0) {
                results = pstmt.executeBatch();
                System.out.println("最终批处理执行结果: " + java.util.Arrays.toString(results));
            }
            
            System.out.println("总共插入 " + data.size() + " 条数据到表: " + tableName);
            
            // 立即提交这批数据
            connection.commit();
            System.out.println("数据已提交到表: " + tableName);
            
        } catch (SQLException e) {
            System.err.println("插入数据失败: " + e.getMessage());
            connection.rollback();
            throw e;
        }
    }
    
    /**
     * 解析Excel数据
     */
    private static void parseExcelData(Sheet sheet, String firstCategory, String secondCategory, 
                                     List<String[]> salesData, List<String[]> volumeData) {
        
        int rowCount = sheet.getPhysicalNumberOfRows();
        System.out.println("总行数: " + rowCount);
        
        // 寻找数据开始行（跳过表头）
        int dataStartRow = findDataStartRow(sheet);
        System.out.println("数据开始行: " + dataStartRow);
        
        if (dataStartRow == -1) {
            System.out.println("未找到数据开始行，尝试从第3行开始");
            dataStartRow = 2;
        }
        
        // 从数据开始行处理，每4行为一组
        for (int i = dataStartRow; i < rowCount; i += 4) {
            if (i + 3 < rowCount) {
                processDataGroup(sheet, i, firstCategory, secondCategory, salesData, volumeData);
            } else {
                // 处理最后不足4行的数据
                processDataGroup(sheet, i, firstCategory, secondCategory, salesData, volumeData);
                break;
            }
        }
        
        System.out.println("解析完成，销售额数据: " + salesData.size() + " 条，销量数据: " + volumeData.size() + " 条");
    }
    
    /**
     * 寻找数据开始行
     */
    private static int findDataStartRow(Sheet sheet) {
        for (int i = 0; i < Math.min(15, sheet.getPhysicalNumberOfRows()); i++) {
            Row row = sheet.getRow(i);
            if (row != null) {
                Cell firstCell = row.getCell(0);
                if (firstCell != null) {
                    String cellValue = getCellValueAsString(firstCell).trim();
                    System.out.println("第" + (i+1) + "行第一列: " + cellValue);
                    
                    // 寻找包含"类目"的行，数据从下一行开始
                    if (cellValue.contains("类目")) {
                        // 跳过表头，寻找第一个实际数据行
                        for (int j = i + 1; j < Math.min(i + 5, sheet.getPhysicalNumberOfRows()); j++) {
                            Row dataRow = sheet.getRow(j);
                            if (dataRow != null && dataRow.getCell(0) != null) {
                                String dataValue = getCellValueAsString(dataRow.getCell(0)).trim();
                                if (!dataValue.isEmpty() && !dataValue.contains("类目") && !dataValue.contains("平台")) {
                                    return j;
                                }
                            }
                        }
                    }
                }
            }
        }
        return -1;
    }
    
    /**
     * 处理一组数据（4行）
     */
    private static void processDataGroup(Sheet sheet, int startRow, String firstCategory, String secondCategory,
                                       List<String[]> salesData, List<String[]> volumeData) {
        
        Row row1 = sheet.getRow(startRow);     // 实际数值行
        Row row2 = sheet.getRow(startRow + 1); // 可能的标识行或空行
        Row row3 = sheet.getRow(startRow + 2); // 同比数值行
        Row row4 = sheet.getRow(startRow + 3); // 同比百分比行
        
        if (row1 == null) return;
        
        // 获取三级类目
        String thirdCategory = extractThirdCategory(row1, row2);
        if (thirdCategory.isEmpty()) {
            System.out.println("跳过空的三级类目，行: " + (startRow + 1));
            return;
        }
        
        System.out.println("处理三级类目: " + thirdCategory);
        
        // 创建销售额数据记录
        String[] salesRecord = new String[FIELDS.length];
        salesRecord[0] = firstCategory;
        salesRecord[1] = secondCategory;
        salesRecord[2] = thirdCategory;
        
        // 创建销量数据记录
        String[] volumeRecord = new String[FIELDS.length];
        volumeRecord[0] = firstCategory;
        volumeRecord[1] = secondCategory;
        volumeRecord[2] = thirdCategory;
        
        // 动态检测Excel格式（检查I列是否有拼多多数据）
        boolean hasPinduoduo = detectPinduoduoFormat(row1);
        System.out.println("检测到Excel格式: " + (hasPinduoduo ? "包含拼多多(8列)" : "不含拼多多(7列)"));
        
        // 填充平台数据
        fillPlatformData(row1, salesRecord, volumeRecord, 3, hasPinduoduo); // 实际数值
        if (row3 != null) {
            fillPlatformData(row3, salesRecord, volumeRecord, 11, hasPinduoduo); // 同比数值
        }
        if (row4 != null) {
            fillPlatformData(row4, salesRecord, volumeRecord, 19, hasPinduoduo); // 同比百分比
        }
        
        salesData.add(salesRecord);
        volumeData.add(volumeRecord);
    }
    
    /**
     * 检测Excel是否包含拼多多列
     */
    private static boolean detectPinduoduoFormat(Row row) {
        if (row == null) return false;
        
        // 检查I列（索引8）是否有数据且不为空
        Cell cell = row.getCell(8);
        if (cell != null) {
            String value = getCellValueAsString(cell).trim();
            // 如果I列有非空数据，认为包含拼多多列
            return !value.isEmpty() && !value.equals("-");
        }
        return false;
    }
    
    /**
     * 提取三级类目
     */
    private static String extractThirdCategory(Row row1, Row row2) {
        Cell firstCell = row1.getCell(0);
        if (firstCell == null) return "";
        
        String cellValue = getCellValueAsString(firstCell).trim();
        
        // 检查是否包含"(所选类目整体)"
        if (cellValue.contains("(所选类目整体)")) {
            return "所选类目整体";
        }
        
        // 如果没有特殊标识，检查是否为空或无意义内容
        if (cellValue.isEmpty() || cellValue.equals("-") || cellValue.toLowerCase().contains("小计") || 
            cellValue.toLowerCase().contains("总计") || cellValue.toLowerCase().contains("合计")) {
            return "";
        }
        
        // 直接返回单元格内容作为三级类目
        return cellValue;
    }
    
    /**
     * 填充平台数据
     */
    private static void fillPlatformData(Row row, String[] salesRecord, String[] volumeRecord, 
                                       int startIndex, boolean hasPinduoduo) {
        if (row == null) return;
        
        // 根据是否包含拼多多来决定读取策略
        int platformCount = hasPinduoduo ? 8 : 7; // 平台数量
        int volumeStartCol = 11; // 销量数据起始列（都从L列开始）
        
        for (int i = 0; i < 8; i++) { // 总是循环8次，确保所有字段都被处理
            if (i < platformCount) {
                // 销售额数据（左半部分）
                int salesColIndex = i + 1; // 从B列开始（A列是类目名称）
                Cell salesCell = row.getCell(salesColIndex);
                String salesValue = "";
                if (salesCell != null) {
                    salesValue = getCellValueAsString(salesCell).trim();
                }
                salesRecord[startIndex + i] = salesValue.isEmpty() ? "" : salesValue;
                
                // 销量数据（右半部分）
                int volumeColIndex = i + volumeStartCol;
                Cell volumeCell = row.getCell(volumeColIndex);
                String volumeValue = "";
                if (volumeCell != null) {
                    volumeValue = getCellValueAsString(volumeCell).trim();
                }
                volumeRecord[startIndex + i] = volumeValue.isEmpty() ? "" : volumeValue;
                

                
            } else {
                // 如果没有拼多多列，拼多多字段设为空
                salesRecord[startIndex + i] = "";
                volumeRecord[startIndex + i] = "";

            }
        }
    }
    
    /**
     * 获取单元格值作为字符串
     */
    private static String getCellValueAsString(Cell cell) {
        if (cell == null) return "";
        
        try {
            switch (cell.getCellType()) {
                case STRING:
                    return cell.getStringCellValue();
                case NUMERIC:
                    if (DateUtil.isCellDateFormatted(cell)) {
                        return cell.getDateCellValue().toString();
                    } else {
                        double numValue = cell.getNumericCellValue();
                        // 如果是整数，不显示小数点
                        if (numValue == (long) numValue) {
                            return String.valueOf((long) numValue);
                        } else {
                            return String.valueOf(numValue);
                        }
                    }
                case BOOLEAN:
                    return String.valueOf(cell.getBooleanCellValue());
                case FORMULA:
                    try {
                        // 尝试获取公式的计算结果
                        return getCellValueAsString(cell.getCachedFormulaResultType(), cell);
                    } catch (Exception e) {
                        return cell.getCellFormula();
                    }
                case BLANK:
                    return "";
                default:
                    return "";
            }
        } catch (Exception e) {
            System.err.println("读取单元格值出错: " + e.getMessage());
            return "";
        }
    }
    
    /**
     * 获取公式计算结果
     */
    private static String getCellValueAsString(CellType cellType, Cell cell) {
        switch (cellType) {
            case STRING:
                return cell.getRichStringCellValue().getString();
            case NUMERIC:
                double numValue = cell.getNumericCellValue();
                if (numValue == (long) numValue) {
                    return String.valueOf((long) numValue);
                } else {
                    return String.valueOf(numValue);
                }
            case BOOLEAN:
                return String.valueOf(cell.getBooleanCellValue());
            default:
                return "";
        }
    }
}