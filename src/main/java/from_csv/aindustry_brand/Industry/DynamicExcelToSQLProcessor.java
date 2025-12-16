package from_csv.aindustry_brand.Industry;

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


//暂时不用这个
//除了行业1月的特殊，都是左右
//其余的文件夹后不带数字的是 正常数据，其他是左右
public class DynamicExcelToSQLProcessor {
    
    // 数据库连接信息
    private static final String DB_SERVER = "192.168.3.183";
    private static final String DB_NAME = "sourcedate";
    private static final String DB_USER = "sa";
    private static final String DB_PASSWORD = "smartpthdata";
    private static final String DB_URL = "jdbc:sqlserver://" + DB_SERVER + ":1433;databaseName=" + DB_NAME + ";encrypt=false;trustServerCertificate=true;";
    
    private final String tableName;
    private Connection connection;
    private Set<String> currentTableColumns;
    
    public DynamicExcelToSQLProcessor(String tableName) {
        this.tableName = tableName;
        this.currentTableColumns = new LinkedHashSet<>();
    }
    
    public static void main(String[] args) {
        String tableName = "ecommerce_insight_new_20250717"; // 可以修改为用户指定的表名
        
        try {
            DynamicExcelToSQLProcessor processor = new DynamicExcelToSQLProcessor(tableName);
            processor.connectToDatabase();
            
            // 处理文件夹中的所有Excel文件
            String basePath = "D:\\wzza\\develop\\idea_project\\ceshi\\src\\main\\java\\from_csv\\aindustry_brand\\Industry\\全域洞察1-5(全部更新版)\\2025-1行业洞察"; // 根据实际情况调整路径
            processor.processExcelFiles(basePath);
            
            System.out.println("动态处理完成！数据已插入到表: " + tableName);
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
     * 处理指定路径下的所有Excel文件
     */
    public void processExcelFiles(String basePath) throws IOException, SQLException {
        File baseDir = new File(basePath);
        
        if (!baseDir.exists()) {
            System.out.println("路径不存在: " + baseDir.getAbsolutePath());
            return;
        }
        
        System.out.println("开始处理路径: " + baseDir.getAbsolutePath());
        
        // 遍历子文件夹（一级类目）
        File[] subDirs = baseDir.listFiles(File::isDirectory);
        
        if (subDirs != null && subDirs.length > 0) {
            for (File subDir : subDirs) {
                String firstCategory = subDir.getName();
                System.out.println("处理一级类目: " + firstCategory);
                processFirstCategoryDirectory(subDir, firstCategory);
            }
        } else {
            System.out.println("没有找到子文件夹");
        }
        
        // 提交事务
        connection.commit();
        closeDatabase();
        System.out.println("所有文件处理完成！");
    }
    
    /**
     * 处理一级类目目录
     */
    private void processFirstCategoryDirectory(File firstCategoryDir, String firstCategory) throws IOException, SQLException {
        // 遍历Excel文件
        File[] excelFiles = firstCategoryDir.listFiles((dir, name) -> 
            name.toLowerCase().endsWith(".xlsx") || name.toLowerCase().endsWith(".xls"));
        
        if (excelFiles != null && excelFiles.length > 0) {
            System.out.println("找到 " + excelFiles.length + " 个Excel文件");
            for (File excelFile : excelFiles) {
                String fileName = excelFile.getName();
                String secondCategory = fileName.substring(0, fileName.lastIndexOf('.'));
                System.out.println("处理Excel文件: " + fileName + " -> 二级类目: " + secondCategory);
                processExcelFile(excelFile, firstCategory, secondCategory);
            }
        } else {
            System.out.println("在目录 " + firstCategoryDir.getName() + " 下没有找到Excel文件");
        }
    }
    
    /**
     * 处理单个Excel文件
     */
    private void processExcelFile(File excelFile, String firstCategory, String secondCategory) throws IOException, SQLException {
        System.out.println("正在处理文件: " + excelFile.getAbsolutePath());
        
        try (FileInputStream fis = new FileInputStream(excelFile)) {
            Workbook workbook;
            
            if (excelFile.getName().toLowerCase().endsWith(".xlsx")) {
                workbook = new XSSFWorkbook(fis);
            } else {
                workbook = new HSSFWorkbook(fis);
            }
            
            // 处理第一个工作表
            Sheet sheet = workbook.getSheetAt(0);
            
            // 读取表头
            Row headerRow = sheet.getRow(0);
            if (headerRow == null) {
                System.out.println("Excel文件没有表头，跳过: " + excelFile.getName());
                workbook.close();
                return;
            }
            
            // 构建完整字段列表（固定字段 + Excel字段）
            List<String> excelColumns = readExcelHeader(headerRow);
            List<String> allColumns = new ArrayList<>();
            allColumns.add("一级类目");
            allColumns.add("二级类目");
            allColumns.addAll(excelColumns);
            
            // 确保数据库表存在并包含所有必要字段
            ensureTableExists(allColumns);
            
            // 读取并插入数据
            List<List<String>> dataRows = readExcelData(sheet, excelColumns.size());
            insertDataToDatabase(dataRows, firstCategory, secondCategory, allColumns);
            
            workbook.close();
            
        } catch (Exception e) {
            System.err.println("处理文件失败: " + excelFile.getAbsolutePath() + " - " + e.getMessage());
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
        
        System.out.println("读取到Excel表头: " + columns);
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
        
        System.out.println("读取到 " + dataRows.size() + " 行数据");
        return dataRows;
    }
    
    /**
     * 确保数据库表存在并包含所有必要字段
     */
    private void ensureTableExists(List<String> allColumns) throws SQLException {
        // 检查表是否存在
        if (!tableExists()) {
            createTable(allColumns);
        } else {
            // 表存在，检查是否需要添加新字段
            loadCurrentTableColumns();
            addMissingColumns(allColumns);
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
     * 创建数据库表
     */
    private void createTable(List<String> columns) throws SQLException {
        StringBuilder sql = new StringBuilder();
        sql.append("CREATE TABLE [").append(tableName).append("] (");
        
        for (int i = 0; i < columns.size(); i++) {
            if (i > 0) sql.append(", ");
            sql.append("[").append(columns.get(i)).append("] NVARCHAR(MAX)");
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
     * 加载当前表的字段列表
     */
    private void loadCurrentTableColumns() throws SQLException {
        currentTableColumns.clear();
        
        String sql = "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = ? ORDER BY ORDINAL_POSITION";
        try (PreparedStatement pstmt = connection.prepareStatement(sql)) {
            pstmt.setString(1, tableName);
            try (ResultSet rs = pstmt.executeQuery()) {
                while (rs.next()) {
                    currentTableColumns.add(rs.getString("COLUMN_NAME"));
                }
            }
        }
        
        System.out.println("当前表字段: " + currentTableColumns);
    }
    
    /**
     * 添加缺失的字段
     */
    private void addMissingColumns(List<String> allColumns) throws SQLException {
        for (String column : allColumns) {
            if (!currentTableColumns.contains(column)) {
                String sql = "ALTER TABLE [" + tableName + "] ADD [" + column + "] NVARCHAR(MAX)";
                System.out.println("添加新字段: " + sql);
                
                try (Statement stmt = connection.createStatement()) {
                    stmt.executeUpdate(sql);
                    currentTableColumns.add(column);
                    System.out.println("字段 " + column + " 添加成功！");
                }
            }
        }
        
        connection.commit();
    }
    
    /**
     * 插入数据到数据库
     */
    private void insertDataToDatabase(List<List<String>> dataRows, String firstCategory, 
                                    String secondCategory, List<String> allColumns) throws SQLException {
        if (dataRows.isEmpty()) {
            System.out.println("没有数据需要插入");
            return;
        }
        
        // 构建INSERT语句
        StringBuilder sql = new StringBuilder();
        sql.append("INSERT INTO [").append(tableName).append("] (");
        
        for (int i = 0; i < allColumns.size(); i++) {
            if (i > 0) sql.append(", ");
            sql.append("[").append(allColumns.get(i)).append("]");
        }
        
        sql.append(") VALUES (");
        
        for (int i = 0; i < allColumns.size(); i++) {
            if (i > 0) sql.append(", ");
            sql.append("?");
        }
        
        sql.append(")");
        
        System.out.println("执行SQL: " + sql.toString());
        
        try (PreparedStatement pstmt = connection.prepareStatement(sql.toString())) {
            int batchCount = 0;
            
            for (List<String> rowData : dataRows) {
                // 设置固定字段
                pstmt.setString(1, firstCategory);
                pstmt.setString(2, secondCategory);
                
                // 设置Excel数据字段
                for (int i = 0; i < rowData.size(); i++) {
                    String value = rowData.get(i);
                    pstmt.setString(i + 3, value); // +3 因为前面有两个固定字段
                }
                
                // 如果Excel字段少于表字段，剩余字段设为空
                for (int i = rowData.size() + 2; i < allColumns.size(); i++) {
                    pstmt.setString(i + 1, "");
                }
                
                pstmt.addBatch();
                batchCount++;
                
                if (batchCount % 1000 == 0) {
                    int[] results = pstmt.executeBatch();
                    System.out.println("已插入 " + batchCount + " 条数据");
                }
            }
            
            // 执行剩余的批处理
            if (batchCount % 1000 != 0) {
                int[] results = pstmt.executeBatch();
            }
            
            System.out.println("总共插入 " + dataRows.size() + " 条数据");
            connection.commit();
            System.out.println("数据已提交到表: " + tableName);
            
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
    

} 