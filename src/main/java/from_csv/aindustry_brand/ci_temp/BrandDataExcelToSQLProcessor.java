package from_csv.aindustry_brand.ci_temp;

import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.ss.usermodel.*;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.sql.*;
import java.util.*;

public class BrandDataExcelToSQLProcessor {
    
    // 配置参数 - 方便修改
    private static final String YM_VALUE = "202507"; // 可修改
    private static final String CHANNEL_VALUE = "天猫"; // 可修改
    
    // 数据库连接信息
    private static final String DB_SERVER = "192.168.3.183";
    private static final String DB_NAME = "sourcedate";
    private static final String DB_USER = "sa";
    private static final String DB_PASSWORD = "smartpthdata";
    private static final String DB_URL = "jdbc:sqlserver://" + DB_SERVER + ":1433;databaseName=" + DB_NAME + ";encrypt=false;trustServerCertificate=true;";
    
    // 目标表字段
    private static final List<String> TARGET_COLUMNS = Arrays.asList(
        "YM", "Channel", "一级类目", "二级类目", "品牌名称", "排名", "上轮排名", "销售额", "销量"
    );
    
    // 固定的列索引（基于你提供的表头）
    private static final int COL_RANK = 0;        // A列：排名
    private static final int COL_BRAND = 1;       // B列：品牌名称
    private static final int COL_PREV_RANK = 2;   // C列：上轮排名
    private static final int COL_CURRENT_QTY = 3; // D列：202507销量
    private static final int COL_CURRENT_AMT = 4; // E列：202507销售额
    
    private final String tableName;
    private Connection connection;
    private List<FileProcessResult> processResults;
    
    public BrandDataExcelToSQLProcessor(String tableName) {
        this.tableName = tableName;
        this.processResults = new ArrayList<>();
    }
    
    public static void main(String[] args) {
        String tableName = "brand_data_202507_20250820"; // 可修改表名
        String basePath = "D:\\wzza\\develop\\idea_project\\ceshi\\src\\main\\java\\from_csv\\aindustry_brand\\ci_temp\\2025天猫数据 - 7月"; // 修改为实际路径
        
        try {
            BrandDataExcelToSQLProcessor processor = new BrandDataExcelToSQLProcessor(tableName);
            processor.connectToDatabase();
            processor.processAllBrandFiles(basePath);
            processor.generateStatisticsReport();
            
            System.out.println("品牌数据处理完成！");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
    /**
     * 连接数据库
     */
    private void connectToDatabase() throws SQLException {
        try {
            Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
            connection = DriverManager.getConnection(DB_URL, DB_USER, DB_PASSWORD);
            connection.setAutoCommit(false);
            System.out.println("数据库连接成功！");
        } catch (ClassNotFoundException e) {
            throw new SQLException("找不到SQLServer驱动程序", e);
        }
    }
    
    /**
     * 处理所有品牌文件
     */
    public void processAllBrandFiles(String basePath) throws IOException, SQLException {
        File baseDir = new File(basePath);
        
        if (!baseDir.exists()) {
            System.out.println("路径不存在: " + baseDir.getAbsolutePath());
            return;
        }
        
        // 确保表存在
        ensureTableExists();
        
        // 遍历小文件夹（一级类目）
        File[] subDirs = baseDir.listFiles(File::isDirectory);
        
        if (subDirs != null) {
            for (File subDir : subDirs) {
                String firstCategory = subDir.getName();
                System.out.println("处理一级类目: " + firstCategory);
                processFirstCategoryDirectory(subDir, firstCategory);
            }
        }
        
        connection.commit();
        closeDatabase();
    }
    
    /**
     * 处理一级类目目录
     */
    private void processFirstCategoryDirectory(File firstCategoryDir, String firstCategory) throws IOException, SQLException {
        File[] excelFiles = firstCategoryDir.listFiles((dir, name) -> 
            name.toLowerCase().endsWith(".xlsx") || name.toLowerCase().endsWith(".xls"));
        
        if (excelFiles != null) {
            for (File excelFile : excelFiles) {
                String fileName = excelFile.getName();
                String secondCategory = fileName.substring(0, fileName.lastIndexOf('.'));
                System.out.println("处理文件: " + fileName);
                
                FileProcessResult result = processExcelFile(excelFile, firstCategory, secondCategory);
                processResults.add(result);
            }
        }
    }
    
    /**
     * 处理单个Excel文件
     */
    private FileProcessResult processExcelFile(File excelFile, String firstCategory, String secondCategory) {
        FileProcessResult result = new FileProcessResult(firstCategory, secondCategory, excelFile.getName());
        
        try (FileInputStream fis = new FileInputStream(excelFile)) {
            Workbook workbook;
            
            if (excelFile.getName().toLowerCase().endsWith(".xlsx")) {
                workbook = new XSSFWorkbook(fis);
            } else {
                workbook = new HSSFWorkbook(fis);
            }
            
            // 查找"品牌"或"sheet2"sheet
            Sheet brandSheet = null;
            for (int i = 0; i < workbook.getNumberOfSheets(); i++) {
                Sheet sheet = workbook.getSheetAt(i);
                String sheetName = sheet.getSheetName();
                if ("品牌".equals(sheetName) || "sheet2".equalsIgnoreCase(sheetName)) {
                    brandSheet = sheet;
                    break;
                }
            }
            
            if (brandSheet == null) {
                result.setError("未找到'品牌'或'sheet2'sheet");
                workbook.close();
                return result;
            }
            
            // 验证表头格式并获取表头行索引
            int headerRowIndex = validateAndFindHeader(brandSheet, excelFile.getName());
            if (headerRowIndex == -1) {
                result.setError("表头格式不匹配标准格式");
                workbook.close();
                return result;
            }
            
            // 读取数据
            List<Map<String, String>> dataRows = readBrandData(brandSheet, headerRowIndex);
            result.setDataRowCount(dataRows.size());
            
            if (!dataRows.isEmpty()) {
                insertDataToDatabase(dataRows, firstCategory, secondCategory);
                result.setSuccess(true);
            } else {
                result.setError("无有效数据");
            }
            
            workbook.close();
            
        } catch (Exception e) {
            result.setError("处理异常: " + e.getMessage());
            e.printStackTrace();
        }
        
        return result;
    }
    
    /**
     * 验证表头格式并返回表头行索引
     */
    private int validateAndFindHeader(Sheet sheet, String fileName) {
        try {
            // 检查前3行，寻找有效的表头
            for (int rowIndex = 0; rowIndex < Math.min(3, sheet.getPhysicalNumberOfRows()); rowIndex++) {
                Row currentRow = sheet.getRow(rowIndex);
                if (currentRow == null) continue;
                
                // 验证关键字段
                String col0 = getCellValueAsString(currentRow.getCell(0)).trim();
                String col1 = getCellValueAsString(currentRow.getCell(1)).trim();
                String col2 = getCellValueAsString(currentRow.getCell(2)).trim();
                String col3 = getCellValueAsString(currentRow.getCell(3)).trim();
                
                // 检查是否匹配标准表头格式
                if ("排名".equals(col0) && "品牌名称".equals(col1) && "上轮排名".equals(col2) && YM_VALUE.equals(col3)) {
                    // 检查下一行的销量、销售额（如果存在）
                    Row nextRow = sheet.getRow(rowIndex + 1);
                    if (nextRow != null) {
                        String col3_next = getCellValueAsString(nextRow.getCell(3)).trim();
                        String col4_next = getCellValueAsString(nextRow.getCell(4)).trim();
                        
                        if ("销量".equals(col3_next) && "销售额".equals(col4_next)) {
                            System.out.println("文件 " + fileName + " 找到有效表头在第 " + (rowIndex + 1) + " 行");
                            return rowIndex; // 返回表头行索引
                        }
                    }
                }
            }
            
            // 如果没有找到标准表头，输出详细信息
            System.err.println("文件 " + fileName + " 表头验证失败，前3行内容如下：");
            for (int i = 0; i < Math.min(3, sheet.getPhysicalNumberOfRows()); i++) {
                Row row = sheet.getRow(i);
                if (row != null) {
                    StringBuilder rowContent = new StringBuilder();
                    for (int j = 0; j < Math.min(5, row.getLastCellNum()); j++) {
                        Cell cell = row.getCell(j);
                        String value = getCellValueAsString(cell).trim();
                        rowContent.append("[").append(j).append("]").append(value).append(" | ");
                    }
                    System.err.println("第 " + (i + 1) + " 行: " + rowContent.toString());
                }
            }
            
            return -1; // 表示未找到有效表头
            
        } catch (Exception e) {
            System.err.println("文件 " + fileName + " 表头验证异常: " + e.getMessage());
            return -1;
        }
    }
    
    /**
     * 读取品牌数据（支持动态表头位置）
     */
    private List<Map<String, String>> readBrandData(Sheet sheet, int headerRowIndex) {
        List<Map<String, String>> dataRows = new ArrayList<>();
        int totalRows = sheet.getPhysicalNumberOfRows();
        
        // 从表头后第2行开始读取数据（跳过表头和子表头）
        int startRow = headerRowIndex + 2;
        
        for (int i = startRow; i < totalRows; i++) {
            Row currentRow = sheet.getRow(i);
            if (currentRow == null) continue;
            
            // 获取品牌名称，如果为空则跳过
            String brandName = getCellValueAsString(currentRow.getCell(COL_BRAND)).trim();
            if (brandName.isEmpty()) continue;
            
            // 过滤总计行
            if ("总计".equals(brandName) || brandName.contains("总计") || brandName.contains("合计")) {
                System.out.println("跳过总计行: " + brandName);
                continue;
            }
            
            Map<String, String> rowData = new HashMap<>();
            
            // 读取排名
            String rank = getCellValueAsString(currentRow.getCell(COL_RANK)).trim();
            // 如果排名为空，检查下一行（处理跨行数据）
            if (rank.isEmpty() && i + 1 < totalRows) {
                Row nextRow = sheet.getRow(i + 1);
                if (nextRow != null) {
                    rank = getCellValueAsString(nextRow.getCell(COL_RANK)).trim();
                }
            }
            
            // 读取上轮排名
            String prevRank = getCellValueAsString(currentRow.getCell(COL_PREV_RANK)).trim();
            if (prevRank.isEmpty() && i + 1 < totalRows) {
                Row nextRow = sheet.getRow(i + 1);
                if (nextRow != null) {
                    prevRank = getCellValueAsString(nextRow.getCell(COL_PREV_RANK)).trim();
                }
            }
            
            // 读取销量
            String quantity = getCellValueAsString(currentRow.getCell(COL_CURRENT_QTY)).trim();
            if (quantity.isEmpty() && i + 1 < totalRows) {
                Row nextRow = sheet.getRow(i + 1);
                if (nextRow != null) {
                    quantity = getCellValueAsString(nextRow.getCell(COL_CURRENT_QTY)).trim();
                }
            }
            
            // 读取销售额
            String amount = getCellValueAsString(currentRow.getCell(COL_CURRENT_AMT)).trim();
            if (amount.isEmpty() && i + 1 < totalRows) {
                Row nextRow = sheet.getRow(i + 1);
                if (nextRow != null) {
                    amount = getCellValueAsString(nextRow.getCell(COL_CURRENT_AMT)).trim();
                }
            }
            
            // 只有品牌名称不为空才添加数据
            rowData.put("品牌名称", brandName);
            rowData.put("排名", rank);
            rowData.put("上轮排名", prevRank);
            rowData.put("销量", quantity);
            rowData.put("销售额", amount);
            
            dataRows.add(rowData);
        }
        
        System.out.println("读取到 " + dataRows.size() + " 行有效品牌数据");
        return dataRows;
    }
    
    /**
     * 确保表存在
     */
    private void ensureTableExists() throws SQLException {
        if (!tableExists()) {
            createTable();
        }
    }
    
    /**
     * 检查表是否存在
     */
    private boolean tableExists() throws SQLException {
        String sql = "SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = ?";
        try (PreparedStatement pstmt = connection.prepareStatement(sql)) {
            pstmt.setString(1, tableName);
            try (ResultSet rs = pstmt.executeQuery()) {
                if (rs.next()) {
                    return rs.getInt(1) > 0;
                }
            }
        }
        return false;
    }
    
    /**
     * 创建表
     */
    private void createTable() throws SQLException {
        StringBuilder sql = new StringBuilder();
        sql.append("CREATE TABLE [").append(tableName).append("] (");
        
        for (int i = 0; i < TARGET_COLUMNS.size(); i++) {
            if (i > 0) sql.append(", ");
            sql.append("[").append(TARGET_COLUMNS.get(i)).append("] NVARCHAR(MAX)");
        }
        
        sql.append(")");
        
        try (Statement stmt = connection.createStatement()) {
            stmt.executeUpdate(sql.toString());
            connection.commit();
            System.out.println("表 " + tableName + " 创建成功！");
        }
    }
    
    /**
     * 插入数据到数据库
     */
    private void insertDataToDatabase(List<Map<String, String>> dataRows, String firstCategory, String secondCategory) throws SQLException {
        StringBuilder sql = new StringBuilder();
        sql.append("INSERT INTO [").append(tableName).append("] (");
        
        for (int i = 0; i < TARGET_COLUMNS.size(); i++) {
            if (i > 0) sql.append(", ");
            sql.append("[").append(TARGET_COLUMNS.get(i)).append("]");
        }
        
        sql.append(") VALUES (");
        
        for (int i = 0; i < TARGET_COLUMNS.size(); i++) {
            if (i > 0) sql.append(", ");
            sql.append("?");
        }
        
        sql.append(")");
        
        try (PreparedStatement pstmt = connection.prepareStatement(sql.toString())) {
            for (Map<String, String> rowData : dataRows) {
                for (int i = 0; i < TARGET_COLUMNS.size(); i++) {
                    String fieldName = TARGET_COLUMNS.get(i);
                    String value = "";
                    
                    switch (fieldName) {
                        case "YM":
                            value = YM_VALUE;
                            break;
                        case "Channel":
                            value = CHANNEL_VALUE;
                            break;
                        case "一级类目":
                            value = firstCategory;
                            break;
                        case "二级类目":
                            value = secondCategory;
                            break;
                        default:
                            value = rowData.getOrDefault(fieldName, "");
                            break;
                    }
                    
                    pstmt.setString(i + 1, value);
                }
                
                pstmt.addBatch();
            }
            
            pstmt.executeBatch();
            connection.commit();
        }
    }
    
    /**
     * 生成统计报告
     */
    private void generateStatisticsReport() throws IOException {
        // 创建统计Excel文件
        Workbook workbook = new XSSFWorkbook();
        Sheet sheet = workbook.createSheet("处理统计");
        
        // 创建表头
        Row headerRow = sheet.createRow(0);
        headerRow.createCell(0).setCellValue("一级类目");
        headerRow.createCell(1).setCellValue("二级类目");
        headerRow.createCell(2).setCellValue("文件名");
        headerRow.createCell(3).setCellValue("处理状态");
        headerRow.createCell(4).setCellValue("数据行数");
        headerRow.createCell(5).setCellValue("错误信息");
        
        // 填充数据
        int rowIndex = 1;
        for (FileProcessResult result : processResults) {
            Row row = sheet.createRow(rowIndex++);
            row.createCell(0).setCellValue(result.firstCategory);
            row.createCell(1).setCellValue(result.secondCategory);
            row.createCell(2).setCellValue(result.fileName);
            row.createCell(3).setCellValue(result.isSuccess ? "成功" : "失败");
            row.createCell(4).setCellValue(result.dataRowCount);
            row.createCell(5).setCellValue(result.errorMessage != null ? result.errorMessage : "");
        }
        
        // 保存文件
        String reportFileName = "brand_data_process_report_" + YM_VALUE + ".xlsx";
        try (FileOutputStream fos = new FileOutputStream(reportFileName)) {
            workbook.write(fos);
        }
        
        workbook.close();
        System.out.println("统计报告已生成: " + reportFileName);
        
        // 打印汇总信息
        long successCount = processResults.stream().filter(r -> r.isSuccess).count();
        long failCount = processResults.stream().filter(r -> !r.isSuccess).count();
        int totalDataRows = processResults.stream().mapToInt(r -> r.dataRowCount).sum();
        
        System.out.println("\n=== 处理汇总 ===");
        System.out.println("总文件数: " + processResults.size());
        System.out.println("成功处理: " + successCount);
        System.out.println("处理失败: " + failCount);
        System.out.println("总数据行数: " + totalDataRows);
        
        // 打印失败的文件
        System.out.println("\n=== 失败文件详情 ===");
        processResults.stream()
            .filter(r -> !r.isSuccess)
            .forEach(r -> System.out.println(r.firstCategory + "/" + r.fileName + ": " + r.errorMessage));
    }
    
    /**
     * 获取单元格值
     */
    private String getCellValueAsString(Cell cell) {
        if (cell == null) return "";
        
        try {
            DataFormatter formatter = new DataFormatter();
            return formatter.formatCellValue(cell).trim();
        } catch (Exception e) {
            return "";
        }
    }
    
    /**
     * 关闭数据库连接
     */
    private void closeDatabase() {
        if (connection != null) {
            try {
                connection.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }
    
    // 内部类：文件处理结果
    private static class FileProcessResult {
        String firstCategory;
        String secondCategory;
        String fileName;
        boolean isSuccess;
        int dataRowCount;
        String errorMessage;
        
        FileProcessResult(String firstCategory, String secondCategory, String fileName) {
            this.firstCategory = firstCategory;
            this.secondCategory = secondCategory;
            this.fileName = fileName;
            this.isSuccess = false;
            this.dataRowCount = 0;
        }
        
        void setSuccess(boolean success) {
            this.isSuccess = success;
        }
        
        void setDataRowCount(int count) {
            this.dataRowCount = count;
        }
        
        void setError(String error) {
            this.errorMessage = error;
            this.isSuccess = false;
        }
    }
}