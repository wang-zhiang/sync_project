package from_csv.aindustry_brand.Industry;

import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.ss.usermodel.*;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.sql.*;
import java.util.*;



//固定字段结构的Excel处理器，支持字段映射
//先用这个
public class FixedSchemaExcelToSQLProcessor {
    
    // 数据库连接信息
    private static final String DB_SERVER = "192.168.3.183";
    private static final String DB_NAME = "sourcedate";
    private static final String DB_USER = "sa";
    private static final String DB_PASSWORD = "smartpthdata";
    private static final String DB_URL = "jdbc:sqlserver://" + DB_SERVER + ":1433;databaseName=" + DB_NAME + ";encrypt=false;trustServerCertificate=true;";
    
    // 固定的表字段结构
    private static final List<String> FIXED_COLUMNS = Arrays.asList(
        "一级类目", "二级类目", "类目名", "全类目名", "平台", 
        "销售额", "销售额占比", "销售额同比增长率", "销售额环比增长率",
        "销量", "销量占比", "销量同比增长率", "销量环比增长率"
    );
    
    // 字段映射表：Excel字段名 -> 标准字段名
    private static final Map<String, String> FIELD_MAPPING = new HashMap<>();
    
    static {
        // 初始化字段映射
        FIELD_MAPPING.put("一级类目", "一级类目");
        FIELD_MAPPING.put("二级类目", "二级类目");
        FIELD_MAPPING.put("类目名", "类目名");
        FIELD_MAPPING.put("全类目名", "全类目名");
        FIELD_MAPPING.put("平台", "平台");
        FIELD_MAPPING.put("销售额", "销售额");
        FIELD_MAPPING.put("销售额占比", "销售额占比");
        FIELD_MAPPING.put("销售额占比 (%)", "销售额占比");
        FIELD_MAPPING.put("销售额同比增长率", "销售额同比增长率");
        FIELD_MAPPING.put("销售额同比增长率 (%)", "销售额同比增长率");
        FIELD_MAPPING.put("销售额环比增长率", "销售额环比增长率");
        FIELD_MAPPING.put("销售额环比增长率 (%)", "销售额环比增长率");
        FIELD_MAPPING.put("销量", "销量");
        FIELD_MAPPING.put("销量占比", "销量占比");
        FIELD_MAPPING.put("销量占比 (%)", "销量占比");
        FIELD_MAPPING.put("销量同比增长率", "销量同比增长率");
        FIELD_MAPPING.put("销量同比增长率 (%)", "销量同比增长率");
        FIELD_MAPPING.put("销量环比增长率", "销量环比增长率");
        FIELD_MAPPING.put("销量环比增长率 (%)", "销量环比增长率");
    }
    
    private final String tableName;
    private Connection connection;
    
    public FixedSchemaExcelToSQLProcessor(String tableName) {
        this.tableName = tableName;
    }
    
    public static void main(String[] args) {

        String tableName = "ecommerce_insight_fixed_202511"; // 可以修改为用户指定的表名
        
        try {
            FixedSchemaExcelToSQLProcessor processor = new FixedSchemaExcelToSQLProcessor(tableName);
            processor.connectToDatabase();
            
            // 处理文件夹中的所有Excel文件
            String basePath = "D:\\wzza\\develop\\idea_project\\ceshi\\src\\main\\java\\from_csv\\aindustry_brand\\Industry\\全域洞察1-5(全部更新版)\\2025-11行业洞察"; // 根据实际情况调整路径
            processor.processExcelFiles(basePath);
            
            System.out.println("固定结构处理完成！数据已插入到表: " + tableName);
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
        
        // 确保表存在
        ensureTableExists();
        
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
            
            // 构建字段映射关系
            Map<String, Integer> fieldIndexMap = buildFieldIndexMap(headerRow);
            
            // 读取并插入数据
            List<Map<String, String>> dataRows = readExcelDataWithMapping(sheet, fieldIndexMap);
            insertDataToDatabase(dataRows, firstCategory, secondCategory);
            
            workbook.close();
            
        } catch (Exception e) {
            System.err.println("处理文件失败: " + excelFile.getAbsolutePath() + " - " + e.getMessage());
            e.printStackTrace();
            connection.rollback();
            throw e;
        }
    }
    
    /**
     * 构建字段索引映射
     */
    private Map<String, Integer> buildFieldIndexMap(Row headerRow) {
        Map<String, Integer> fieldIndexMap = new HashMap<>();
        Map<String, Integer> tempMap = new HashMap<>();
        
        // 首先收集所有字段和其索引
        int lastCellNum = headerRow.getLastCellNum();
        for (int i = 0; i < lastCellNum; i++) {
            Cell cell = headerRow.getCell(i);
            if (cell != null) {
                String columnName = getCellValueAsString(cell).trim();
                if (!columnName.isEmpty() && FIELD_MAPPING.containsKey(columnName)) {
                    tempMap.put(columnName, i);
                }
            }
        }
        
        // 处理字段映射，如果同时存在带(%)和不带(%)的字段，优先使用不带(%)的
        for (String standardField : FIXED_COLUMNS) {
            if (standardField.equals("一级类目") || standardField.equals("二级类目")) {
                continue; // 这两个字段是程序自动填充的
            }
            
            // 查找对应的Excel字段
            String directField = standardField;
            String percentField = standardField + " (%)";
            
            if (tempMap.containsKey(directField)) {
                fieldIndexMap.put(standardField, tempMap.get(directField));
            } else if (tempMap.containsKey(percentField)) {
                fieldIndexMap.put(standardField, tempMap.get(percentField));
            }
        }
        
        System.out.println("字段映射结果: " + fieldIndexMap);
        return fieldIndexMap;
    }
    
    /**
     * 读取Excel数据并应用字段映射
     */
    private List<Map<String, String>> readExcelDataWithMapping(Sheet sheet, Map<String, Integer> fieldIndexMap) {
        List<Map<String, String>> dataRows = new ArrayList<>();
        
        int totalRows = sheet.getPhysicalNumberOfRows();
        for (int i = 1; i < totalRows; i++) { // 跳过表头
            Row row = sheet.getRow(i);
            if (row == null) continue;
            
            Map<String, String> rowData = new HashMap<>();
            boolean hasData = false;
            
            // 按照固定字段顺序读取数据
            for (String standardField : FIXED_COLUMNS) {
                if (standardField.equals("一级类目") || standardField.equals("二级类目")) {
                    continue; // 这两个字段是程序自动填充的
                }
                
                Integer columnIndex = fieldIndexMap.get(standardField);
                if (columnIndex != null) {
                    Cell cell = row.getCell(columnIndex);
                    String value = cell != null ? getCellValueAsString(cell).trim() : "";
                    rowData.put(standardField, value);
                    if (!value.isEmpty()) {
                        hasData = true;
                    }
                } else {
                    rowData.put(standardField, "");
                }
            }
            
            // 只有当行中有数据时才添加
            if (hasData) {
                dataRows.add(rowData);
            }
        }
        
        System.out.println("读取到 " + dataRows.size() + " 行数据");
        return dataRows;
    }
    
    /**
     * 确保数据库表存在
     */
    private void ensureTableExists() throws SQLException {
        // 检查表是否存在
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
     * 创建数据库表
     */
    private void createTable() throws SQLException {
        StringBuilder sql = new StringBuilder();
        sql.append("CREATE TABLE [").append(tableName).append("] (");
        
        for (int i = 0; i < FIXED_COLUMNS.size(); i++) {
            if (i > 0) sql.append(", ");
            sql.append("[").append(FIXED_COLUMNS.get(i)).append("] NVARCHAR(MAX)");
        }
        
        sql.append(")");
        
        System.out.println("创建表: " + sql.toString());
        
        try (Statement stmt = connection.createStatement()) {
            stmt.executeUpdate(sql.toString());
            connection.commit();
            System.out.println("表 " + tableName + " 创建成功！");
        }
    }
    
    /**
     * 插入数据到数据库
     */
    private void insertDataToDatabase(List<Map<String, String>> dataRows, String firstCategory, 
                                    String secondCategory) throws SQLException {
        if (dataRows.isEmpty()) {
            System.out.println("没有数据需要插入");
            return;
        }
        
        // 构建INSERT语句
        StringBuilder sql = new StringBuilder();
        sql.append("INSERT INTO [").append(tableName).append("] (");
        
        for (int i = 0; i < FIXED_COLUMNS.size(); i++) {
            if (i > 0) sql.append(", ");
            sql.append("[").append(FIXED_COLUMNS.get(i)).append("]");
        }
        
        sql.append(") VALUES (");
        
        for (int i = 0; i < FIXED_COLUMNS.size(); i++) {
            if (i > 0) sql.append(", ");
            sql.append("?");
        }
        
        sql.append(")");
        
        System.out.println("执行SQL: " + sql.toString());
        
        try (PreparedStatement pstmt = connection.prepareStatement(sql.toString())) {
            int batchCount = 0;
            
            for (Map<String, String> rowData : dataRows) {
                // 按照固定字段顺序设置参数
                for (int i = 0; i < FIXED_COLUMNS.size(); i++) {
                    String fieldName = FIXED_COLUMNS.get(i);
                    String value = "";
                    
                    if ("一级类目".equals(fieldName)) {
                        value = firstCategory;
                    } else if ("二级类目".equals(fieldName)) {
                        value = secondCategory;
                    } else {
                        value = rowData.getOrDefault(fieldName, "");
                    }
                    
                    pstmt.setString(i + 1, value);
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