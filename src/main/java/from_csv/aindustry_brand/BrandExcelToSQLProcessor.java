package from_csv.aindustry_brand;

import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.ss.usermodel.*;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.sql.*;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * Brand文件夹Excel数据导入器
 * 功能：递归遍历brand文件夹，将所有Excel文件数据导入到统一的数据库表中
 * 添加三个固定字段：一级类目、二级类目、三级类目
 */
public class BrandExcelToSQLProcessor {
    
    // 数据库连接信息
    private static final String DB_SERVER = "192.168.3.183";
    private static final String DB_NAME = "sourcedate";
    private static final String DB_USER = "sa";
    private static final String DB_PASSWORD = "smartpthdata";
    private static final String DB_URL = "jdbc:sqlserver://" + DB_SERVER + ":1433;databaseName=" + DB_NAME + ";encrypt=false;trustServerCertificate=true;";
    
    private final String tableName;
    private Connection connection;
    private Set<String> currentTableColumns;
    
    public BrandExcelToSQLProcessor(String tableName) {
        this.tableName = tableName;
        this.currentTableColumns = new LinkedHashSet<>();
    }
    
    public static void main(String[] args) {
        String tableName = "brand_insights_202510"; // 可以修改为用户指定的表名
        
        try {
            BrandExcelToSQLProcessor processor = new BrandExcelToSQLProcessor(tableName);
            processor.connectToDatabase();
            
            // 处理brand文件夹中的所有Excel文件
            String basePath = "D:\\wzza\\develop\\idea_project\\ceshi\\src\\main\\java\\from_csv\\aindustry_brand\\brand"; // 根据实际情况调整路径
            processor.processBrandExcelFiles(basePath);
            
            System.out.println("Brand数据导入完成！数据已插入到表: " + tableName);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
    /**
     * 连接到数据库
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
     * 关闭数据库连接
     */
    private void closeDatabase() {
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
     * 处理brand文件夹下的所有Excel文件
     */
    public void processBrandExcelFiles(String basePath) throws IOException, SQLException {
        File baseDir = new File(basePath);
        
        if (!baseDir.exists()) {
            System.out.println("Brand路径不存在: " + baseDir.getAbsolutePath());
            return;
        }
        
        System.out.println("开始处理Brand路径: " + baseDir.getAbsolutePath());
        
        // 如果表存在，先删除
        dropTableIfExists();
        
        // 收集所有Excel文件信息
        List<ExcelFileInfo> allExcelFiles = collectAllExcelFiles(baseDir, "");
        
        if (allExcelFiles.isEmpty()) {
            System.out.println("没有找到任何Excel文件");
            return;
        }
        
        System.out.println("找到 " + allExcelFiles.size() + " 个Excel文件");
        
        // 收集所有字段
        Set<String> allColumns = collectAllColumns(allExcelFiles);
        
        // 创建表
        createTable(allColumns);
        
        // 导入所有数据
        int totalRowsInserted = 0;
        for (ExcelFileInfo fileInfo : allExcelFiles) {
            System.out.println("正在处理: " + fileInfo.getDisplayPath());
            int rows = processExcelFile(fileInfo, allColumns);
            totalRowsInserted += rows;
            System.out.println("插入 " + rows + " 行数据");
        }
        
        // 提交事务
        connection.commit();
        closeDatabase();
        System.out.println("所有文件处理完成！总共插入 " + totalRowsInserted + " 行数据");
    }
    
    /**
     * 递归收集所有Excel文件信息
     */
    private List<ExcelFileInfo> collectAllExcelFiles(File directory, String currentPath) {
        List<ExcelFileInfo> excelFiles = new ArrayList<>();
        
        File[] files = directory.listFiles();
        if (files == null) {
            return excelFiles;
        }
        
        for (File file : files) {
            if (file.isDirectory()) {
                // 递归处理子文件夹
                String newPath = currentPath.isEmpty() ? file.getName() : currentPath + "/" + file.getName();
                excelFiles.addAll(collectAllExcelFiles(file, newPath));
            } else if (file.isFile()) {
                String fileName = file.getName().toLowerCase();
                if (fileName.endsWith(".xls") || fileName.endsWith(".xlsx")) {
                    ExcelFileInfo fileInfo = new ExcelFileInfo(file, currentPath);
                    excelFiles.add(fileInfo);
                }
            }
        }
        
        return excelFiles;
    }
    
    /**
     * 收集所有Excel文件的字段
     */
    private Set<String> collectAllColumns(List<ExcelFileInfo> excelFiles) throws IOException {
        Set<String> allColumns = new LinkedHashSet<>();
        
        // 添加固定字段
        allColumns.add("一级类目");
        allColumns.add("二级类目");
        allColumns.add("三级类目");
        
        for (ExcelFileInfo fileInfo : excelFiles) {
            List<String> fileColumns = readExcelColumns(fileInfo.getFile());
            allColumns.addAll(fileColumns);
        }
        
        System.out.println("所有字段: " + allColumns);
        return allColumns;
    }
    
    /**
     * 读取Excel文件的列名
     */
    private List<String> readExcelColumns(File excelFile) throws IOException {
        List<String> columns = new ArrayList<>();
        
        try (FileInputStream fis = new FileInputStream(excelFile)) {
            Workbook workbook;
            
            if (excelFile.getName().toLowerCase().endsWith(".xlsx")) {
                workbook = new XSSFWorkbook(fis);
            } else {
                workbook = new HSSFWorkbook(fis);
            }
            
            // 只读取第一个工作表的表头
            Sheet sheet = workbook.getSheetAt(0);
            Row headerRow = sheet.getRow(0);
            
            if (headerRow != null) {
                int lastCellNum = headerRow.getLastCellNum();
                for (int i = 0; i < lastCellNum; i++) {
                    Cell cell = headerRow.getCell(i);
                    if (cell != null) {
                        String columnName = getCellValueAsString(cell).trim();
                        if (!columnName.isEmpty()) {
                            columns.add(columnName);
                        }
                    }
                }
            }
            
            workbook.close();
        } catch (Exception e) {
            System.err.println("读取Excel列名失败: " + excelFile.getAbsolutePath() + " - " + e.getMessage());
        }
        
        return columns;
    }
    
    /**
     * 删除表（如果存在）
     */
    private void dropTableIfExists() throws SQLException {
        String sql = "IF OBJECT_ID('" + tableName + "', 'U') IS NOT NULL DROP TABLE [" + tableName + "]";
        try (Statement stmt = connection.createStatement()) {
            stmt.executeUpdate(sql);
            connection.commit();
            System.out.println("已删除现有表: " + tableName);
        } catch (SQLException e) {
            System.out.println("删除表失败或表不存在: " + e.getMessage());
        }
    }
    
    /**
     * 创建数据库表
     */
    private void createTable(Set<String> columns) throws SQLException {
        StringBuilder sql = new StringBuilder();
        sql.append("CREATE TABLE [").append(tableName).append("] (");
        
        boolean first = true;
        for (String column : columns) {
            if (!first) sql.append(", ");
            sql.append("[").append(column).append("] NVARCHAR(MAX)");
            first = false;
        }
        
        sql.append(")");
        
        System.out.println("创建表: " + sql.toString());
        
        try (Statement stmt = connection.createStatement()) {
            stmt.executeUpdate(sql.toString());
            connection.commit();
            System.out.println("表 " + tableName + " 创建成功！");
        }
        
        // 更新当前表字段列表
        currentTableColumns.addAll(columns);
    }
    
    /**
     * 处理单个Excel文件
     */
    private int processExcelFile(ExcelFileInfo fileInfo, Set<String> allColumns) throws IOException, SQLException {
        try (FileInputStream fis = new FileInputStream(fileInfo.getFile())) {
            Workbook workbook;
            
            if (fileInfo.getFile().getName().toLowerCase().endsWith(".xlsx")) {
                workbook = new XSSFWorkbook(fis);
            } else {
                workbook = new HSSFWorkbook(fis);
            }
            
            // 处理第一个工作表
            Sheet sheet = workbook.getSheetAt(0);
            
            // 读取表头
            Row headerRow = sheet.getRow(0);
            if (headerRow == null) {
                System.out.println("Excel文件没有表头，跳过: " + fileInfo.getFile().getName());
                workbook.close();
                return 0;
            }
            
            List<String> excelColumns = readExcelHeader(headerRow);
            
            // 读取数据
            List<List<String>> dataRows = readExcelData(sheet, excelColumns.size());
            
            // 插入数据
            int rowsInserted = insertDataToDatabase(dataRows, fileInfo, excelColumns, allColumns);
            
            workbook.close();
            return rowsInserted;
            
        } catch (Exception e) {
            System.err.println("处理文件失败: " + fileInfo.getFile().getAbsolutePath() + " - " + e.getMessage());
            e.printStackTrace();
            connection.rollback();
            throw e;
        }
    }
    
    /**
     * 读取Excel表头
     */
    private List<String> readExcelHeader(Row headerRow) {
        List<String> columns = new ArrayList<>();
        
        int lastCellNum = headerRow.getLastCellNum();
        for (int i = 0; i < lastCellNum; i++) {
            Cell cell = headerRow.getCell(i);
            if (cell != null) {
                String columnName = getCellValueAsString(cell).trim();
                if (!columnName.isEmpty()) {
                    columns.add(columnName);
                }
            }
        }
        
        return columns;
    }
    
    /**
     * 读取Excel数据
     */
    private List<List<String>> readExcelData(Sheet sheet, int columnCount) {
        List<List<String>> dataRows = new ArrayList<>();
        
        int totalRows = sheet.getPhysicalNumberOfRows();
        for (int i = 1; i < totalRows; i++) { // 跳过表头
            Row row = sheet.getRow(i);
            if (row == null) continue;
            
            List<String> rowData = new ArrayList<>();
            for (int j = 0; j < columnCount; j++) {
                Cell cell = row.getCell(j);
                String value = cell != null ? getCellValueAsString(cell).trim() : "";
                rowData.add(value);
            }
            
            // 检查是否为空行
            boolean isEmptyRow = rowData.stream().allMatch(String::isEmpty);
            if (!isEmptyRow) {
                dataRows.add(rowData);
            }
        }
        
        return dataRows;
    }
    
    /**
     * 插入数据到数据库
     */
    private int insertDataToDatabase(List<List<String>> dataRows, ExcelFileInfo fileInfo, 
                                   List<String> excelColumns, Set<String> allColumns) throws SQLException {
        if (dataRows.isEmpty()) {
            return 0;
        }
        
        // 构建INSERT语句
        StringBuilder sql = new StringBuilder();
        sql.append("INSERT INTO [").append(tableName).append("] (");
        
        boolean first = true;
        for (String column : allColumns) {
            if (!first) sql.append(", ");
            sql.append("[").append(column).append("]");
            first = false;
        }
        
        sql.append(") VALUES (");
        
        first = true;
        for (String column : allColumns) {
            if (!first) sql.append(", ");
            sql.append("?");
            first = false;
        }
        
        sql.append(")");
        
        try (PreparedStatement pstmt = connection.prepareStatement(sql.toString())) {
            int batchCount = 0;
            
            for (List<String> rowData : dataRows) {
                int paramIndex = 1;
                
                // 设置固定字段
                pstmt.setString(paramIndex++, fileInfo.getFirstCategory());
                pstmt.setString(paramIndex++, fileInfo.getSecondCategory());
                pstmt.setString(paramIndex++, fileInfo.getThirdCategory());
                
                // 设置Excel数据字段
                for (String column : allColumns) {
                    if (column.equals("一级类目") || column.equals("二级类目") || column.equals("三级类目")) {
                        continue; // 已经设置过了
                    }
                    
                    // 查找该字段在Excel中的位置
                    int excelIndex = excelColumns.indexOf(column);
                    String value = "";
                    if (excelIndex >= 0 && excelIndex < rowData.size()) {
                        value = rowData.get(excelIndex);
                    }
                    
                    pstmt.setString(paramIndex++, value);
                }
                
                pstmt.addBatch();
                batchCount++;
                
                if (batchCount % 1000 == 0) {
                    pstmt.executeBatch();
                }
            }
            
            // 执行剩余的批处理
            if (batchCount % 1000 != 0) {
                pstmt.executeBatch();
            }
            
            return dataRows.size();
            
        } catch (SQLException e) {
            System.err.println("插入数据失败: " + e.getMessage());
            connection.rollback();
            throw e;
        }
    }
    
    /**
     * 获取单元格值作为字符串 - 原封不动地获取Excel中显示的内容
     */
    private String getCellValueAsString(Cell cell) {
        if (cell == null) return "";
        
        try {
            // 使用DataFormatter直接获取Excel中显示的原始格式化文本
            DataFormatter formatter = new DataFormatter();
            return formatter.formatCellValue(cell).trim();
        } catch (Exception e) {
            System.err.println("读取单元格值出错: " + e.getMessage());
            return "";
        }
    }
    
    /**
     * Excel文件信息类
     */
    private static class ExcelFileInfo {
        private final File file;
        private final String path;
        
        public ExcelFileInfo(File file, String path) {
            this.file = file;
            this.path = path;
        }
        
        public File getFile() {
            return file;
        }
        
        public String getFirstCategory() {
            if (path.isEmpty()) return "";
            String[] parts = path.split("/");
            return parts[0];
        }
        
        public String getSecondCategory() {
            if (path.isEmpty()) return "";
            String[] parts = path.split("/");
            return parts.length > 1 ? parts[1] : "";
        }
        
        public String getThirdCategory() {
            String fileName = file.getName();
            int dotIndex = fileName.lastIndexOf('.');
            return dotIndex > 0 ? fileName.substring(0, dotIndex) : fileName;
        }
        
        public String getDisplayPath() {
            return path.isEmpty() ? file.getName() : path + "/" + file.getName();
        }
    }
} 