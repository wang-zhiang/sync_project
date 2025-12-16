package dim;
import org.yaml.snakeyaml.Yaml;
import java.io.FileInputStream;
import java.sql.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

public class DataSyncService {
    private static final int BATCH_SIZE = 20000;
    private static final int THREAD_POOL_SIZE = 6;
    
    private DatabaseManager dbManager;
    private TaskConfig config;

//2点20开始的
    // 主方法示例
    public static void main(String[] args) {
        DataSyncService syncService = new DataSyncService();

        try {
            // 加载配置文件
            syncService.loadConfig("D:\\wzza\\develop\\idea_project\\ceshi\\src\\main\\java\\dim\\task.yaml");

            // 在这里设置要同步的taskId
            String taskId = "sku";  // 你可以在这里修改要同步的taskId

            // 执行同步
            syncService.syncByTaskId(taskId);

        } catch (Exception e) {
            System.err.println("同步失败: " + e.getMessage());
            e.printStackTrace();
        } finally {
            syncService.close();
        }
    }
    
    public DataSyncService() {
        this.dbManager = new DatabaseManager();
    }
    
    public void loadConfig(String configPath) throws Exception {
        Yaml yaml = new Yaml();
        try (FileInputStream fis = new FileInputStream(configPath)) {
            config = yaml.loadAs(fis, TaskConfig.class);
        }
        System.out.println("配置文件加载完成");
    }
    
    public void syncByTaskId(String taskId) throws Exception {
        TaskConfig.Task task = findTaskById(taskId);
        if (task == null) {
            System.err.println("未找到taskId: " + taskId);
            return;
        }
        
        System.out.println("开始同步任务: " + taskId);
        System.out.println("源表: " + task.getSource().getTable());
        System.out.println("目标表: " + task.getTargetTable());
        
        // 初始化数据库连接
        dbManager.initSqlServerConnection(task.getSource());
        dbManager.initClickHouseConnection(config.getClickhouse());
        
        // 获取字段类型信息
        String sourceTableName = task.getSource().getTable().replace("dbo.", "");
        Map<String, String> sourceColumnTypes = dbManager.getSqlServerColumnTypes(sourceTableName);
        Map<String, String> targetColumnTypes = dbManager.getClickHouseColumnTypes(task.getTargetTable());
        
        System.out.println("字段类型映射获取完成");
        
        // 执行同步
        performSync(task, sourceColumnTypes, targetColumnTypes);
        
        System.out.println("任务同步完成: " + taskId);
    }
    
    private TaskConfig.Task findTaskById(String taskId) {
        return config.getTasks().stream()
                .filter(task -> taskId.equals(task.getTaskId()))
                .findFirst()
                .orElse(null);
    }
    
    private void performSync(TaskConfig.Task task, Map<String, String> sourceTypes, 
                           Map<String, String> targetTypes) throws Exception {
        
        // 获取总记录数
        long totalCount = getTotalCount(task);
        System.out.println("总记录数: " + totalCount);
        
        if (totalCount == 0) {
            System.out.println("源表无数据，跳过同步");
            return;
        }
        
        // 清空目标表
        truncateTargetTable(task);
        
        // 计算分页数
        int totalPages = (int) Math.ceil((double) totalCount / BATCH_SIZE);
        System.out.println("分页数: " + totalPages + "，每页: " + BATCH_SIZE + " 条");
        
        // 创建线程池
        ExecutorService executor = Executors.newFixedThreadPool(THREAD_POOL_SIZE);
        List<CompletableFuture<Void>> futures = new ArrayList<>();
        
        // 提交分页任务
        for (int page = 0; page < totalPages; page++) {
            final int currentPage = page;
            CompletableFuture<Void> future = CompletableFuture.runAsync(() -> {
                try {
                    syncPage(task, currentPage, sourceTypes, targetTypes);
                } catch (Exception e) {
                    System.err.println("页面同步失败 [" + currentPage + "]: " + e.getMessage());
                    e.printStackTrace();
                }
            }, executor);
            futures.add(future);
        }
        
        // 等待所有任务完成
        CompletableFuture<Void>[] futureArray = futures.toArray(new CompletableFuture[futures.size()]);
        CompletableFuture.allOf(futureArray).join();
        executor.shutdown();
        
        System.out.println("所有分页同步完成");
    }
    
    private long getTotalCount(TaskConfig.Task task) throws SQLException {
        String sql = "SELECT COUNT(*) FROM " + task.getSource().getTable();
        try (Connection conn = dbManager.getSqlServerConnection();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            return rs.next() ? rs.getLong(1) : 0;
        }
    }
    
    private void truncateTargetTable(TaskConfig.Task task) throws SQLException {
        String sql;
        String tableName = task.getTargetTable();
        
        if (task.getTableType() == 0) {
            // 分布式表：使用_local表名，加上ON CLUSTER
            sql = "TRUNCATE TABLE " + config.getClickhouse().getDatabase() + "." + tableName + "_local ON CLUSTER test_ck_cluster";
            System.out.println("清空分布式表本地表: " + tableName + "_local");
        } else if (task.getTableType() == 1) {
            // 多节点相同表：直接使用表名，加上ON CLUSTER
            sql = "TRUNCATE TABLE " + config.getClickhouse().getDatabase() + "." + tableName + " ON CLUSTER test_ck_cluster";
            System.out.println("清空多节点表: " + tableName);
        } else {
            // 默认处理：普通表
            sql = "TRUNCATE TABLE " + config.getClickhouse().getDatabase() + "." + tableName;
            System.out.println("清空普通表: " + tableName);
        }
        
        try (Connection conn = dbManager.getClickHouseConnection();
             Statement stmt = conn.createStatement()) {
            stmt.execute(sql);
            System.out.println("目标表清空完成，SQL: " + sql);
        }
    }    
 
   private void syncPage(TaskConfig.Task task, int page, Map<String, String> sourceTypes, 
                         Map<String, String> targetTypes) throws SQLException {
        
        long startTime = System.currentTimeMillis();
        int offset = page * BATCH_SIZE;
        
        // 构建主键排序字段
        StringBuilder orderByBuilder = new StringBuilder();
        for (int i = 0; i < task.getPrimaryKeys().size(); i++) {
            if (i > 0) orderByBuilder.append(", ");
            orderByBuilder.append(task.getPrimaryKeys().get(i));
        }
        String orderBy = orderByBuilder.toString();
        
        // 构建源字段列表
        List<String> sourceFields = new ArrayList<>();
        for (TaskConfig.FieldMapping mapping : task.getFieldMappings()) {
            sourceFields.add("[" + mapping.getSourceField() + "]");
        }
        
        // 构建源字段字符串
        StringBuilder sourceFieldsBuilder = new StringBuilder();
        for (int i = 0; i < sourceFields.size(); i++) {
            if (i > 0) sourceFieldsBuilder.append(", ");
            sourceFieldsBuilder.append(sourceFields.get(i));
        }
        
        // 使用ROW_NUMBER()方式分页，兼容老版本SQL Server
        String selectSql = String.format(
            "SELECT %s FROM (" +
            "SELECT %s, ROW_NUMBER() OVER (ORDER BY %s) as rn FROM %s" +
            ") t WHERE rn > %d AND rn <= %d",
            sourceFieldsBuilder.toString(),
            sourceFieldsBuilder.toString(),
            orderBy,
            task.getSource().getTable(),
            offset,
            offset + BATCH_SIZE
        );
        
        // 构建目标字段列表和占位符
        List<String> targetFields = new ArrayList<>();
        List<String> placeholders = new ArrayList<>();
        for (TaskConfig.FieldMapping mapping : task.getFieldMappings()) {
            targetFields.add(mapping.getTargetField());
            placeholders.add("?");
        }
        
        // 构建目标字段字符串
        StringBuilder targetFieldsBuilder = new StringBuilder();
        for (int i = 0; i < targetFields.size(); i++) {
            if (i > 0) targetFieldsBuilder.append(", ");
            targetFieldsBuilder.append(targetFields.get(i));
        }
        
        // 构建占位符字符串
        StringBuilder placeholdersBuilder = new StringBuilder();
        for (int i = 0; i < placeholders.size(); i++) {
            if (i > 0) placeholdersBuilder.append(", ");
            placeholdersBuilder.append(placeholders.get(i));
        }
        
        String insertSql = String.format(
            "INSERT INTO %s (%s) VALUES (%s)",
            task.getTargetTable(),
            targetFieldsBuilder.toString(),
            placeholdersBuilder.toString()
        );
        
        try (Connection sourceConn = dbManager.getSqlServerConnection();
             Connection targetConn = dbManager.getClickHouseConnection();
             PreparedStatement selectStmt = sourceConn.prepareStatement(selectSql);
             PreparedStatement insertStmt = targetConn.prepareStatement(insertSql)) {
            
            ResultSet rs = selectStmt.executeQuery();
            int batchCount = 0;
            int recordCount = 0;
            
            while (rs.next()) {
                // 设置参数值
                for (int i = 0; i < task.getFieldMappings().size(); i++) {
                    TaskConfig.FieldMapping mapping = task.getFieldMappings().get(i);
                    Object value = rs.getObject(mapping.getSourceField());
                    
                    // 数据类型转换处理
                    Object convertedValue = convertValue(value, 
                        sourceTypes.get(mapping.getSourceField()),
                        targetTypes.get(mapping.getTargetField()));
                    
                    insertStmt.setObject(i + 1, convertedValue);
                }
                
                insertStmt.addBatch();
                batchCount++;
                recordCount++;
                
                // 每1000条执行一次批量插入
                if (batchCount >= 1000) {
                    insertStmt.executeBatch();
                    batchCount = 0;
                }
            }
            
            // 执行剩余的批量插入
            if (batchCount > 0) {
                insertStmt.executeBatch();
            }
            
            long endTime = System.currentTimeMillis();
            System.out.printf("页面 [%d] 同步完成，记录数: %d，耗时: %d ms%n", 
                page, recordCount, endTime - startTime);
        }
    }
    
    private Object convertValue(Object value, String sourceType, String targetType) {
        // 处理NULL值和"NULL"字符串
        if (value == null || "NULL".equals(String.valueOf(value).trim())) {
            return getDefaultValueForNull(targetType);
        }
        
        // 只根据ClickHouse目标类型转换，不关心SQL Server源类型
        return convertByClickHouseType(value, targetType);
    }
    
    private Object getDefaultValueForNull(String targetType) {
        if (targetType == null) return null;
        
        // ClickHouse字符串类型 -> 空字符串
        if (targetType.startsWith("String") || targetType.startsWith("FixedString")) {
            return "";
        }
        
        // ClickHouse整数类型 -> 0 或 0L
        if (targetType.startsWith("Int") || targetType.startsWith("UInt")) {
            if (targetType.startsWith("Int64") || targetType.startsWith("UInt64") || 
                targetType.startsWith("Int128") || targetType.startsWith("Int256")) {
                return 0L;
            }
            return 0;
        }
        
        // ClickHouse浮点类型 -> 0.0
        if (targetType.startsWith("Float") || targetType.startsWith("Double") || 
            targetType.startsWith("Decimal")) {
            return 0.0;
        }
        
        // ClickHouse日期类型 -> 默认日期
        if (targetType.startsWith("Date") || targetType.startsWith("DateTime")) {
            return new Timestamp(0); // 1970-01-01
        }
        
        // ClickHouse布尔类型 -> false
        if (targetType.startsWith("Bool")) {
            return false;
        }
        
        return null;
    }
    
    private Object convertByClickHouseType(Object value, String clickHouseType) {
        try {
            // 字符串类型
            if (clickHouseType.startsWith("String") || clickHouseType.startsWith("FixedString")) {
                return value.toString();
            }
            
            // 整数类型
            if (clickHouseType.startsWith("Int8") || clickHouseType.startsWith("UInt8") ||
                clickHouseType.startsWith("Int16") || clickHouseType.startsWith("UInt16") ||
                clickHouseType.startsWith("Int32") || clickHouseType.startsWith("UInt32")) {
                return convertToInt(value);
            }
            
            if (clickHouseType.startsWith("Int64") || clickHouseType.startsWith("UInt64") ||
                clickHouseType.startsWith("Int128") || clickHouseType.startsWith("Int256")) {
                return convertToLong(value);
            }
            
            // 浮点类型
            if (clickHouseType.startsWith("Float32")) {
                return convertToFloat(value);
            }
            
            if (clickHouseType.startsWith("Float64") || clickHouseType.startsWith("Double") ||
                clickHouseType.startsWith("Decimal")) {
                return convertToDouble(value);
            }
            
            // 日期类型
            if (clickHouseType.startsWith("Date") || clickHouseType.startsWith("DateTime")) {
                return convertToTimestamp(value);
            }
            
            // 布尔类型
            if (clickHouseType.startsWith("Bool")) {
                return convertToBoolean(value);
            }
            
        } catch (Exception e) {
            System.err.println("转换失败: " + value + " -> " + clickHouseType + ", 使用默认值");
            return getDefaultValueForNull(clickHouseType);
        }
        
        return value;
    }
    
    private int convertToInt(Object value) {
        if (value instanceof Number) {
            return ((Number) value).intValue();
        }
        String str = value.toString().trim();
        if (str.isEmpty()) return 0;
        
        try {
            // 处理浮点格式的字符串（如"1.0"）
            if (str.contains(".")) {
                return (int) Double.parseDouble(str);
            }
            return Integer.parseInt(str);
        } catch (NumberFormatException e) {
            System.err.println("整数转换失败: " + str + ", 使用默认值0");
            return 0;
        }
    }
    
    private long convertToLong(Object value) {
        if (value instanceof Number) {
            return ((Number) value).longValue();
        }
        String str = value.toString().trim();
        if (str.isEmpty()) return 0L;
        
        try {
            // 处理浮点格式的字符串（如"1.0"）
            if (str.contains(".")) {
                return (long) Double.parseDouble(str);
            }
            return Long.parseLong(str);
        } catch (NumberFormatException e) {
            System.err.println("长整数转换失败: " + str + ", 使用默认值0");
            return 0L;
        }
    }
    
    private float convertToFloat(Object value) {
        if (value instanceof Number) {
            return ((Number) value).floatValue();
        }
        String str = value.toString().trim();
        if (str.isEmpty()) return 0.0f;
        
        try {
            return Float.parseFloat(str);
        } catch (NumberFormatException e) {
            System.err.println("浮点数转换失败: " + str + ", 使用默认值0.0");
            return 0.0f;
        }
    }
    
    private double convertToDouble(Object value) {
        if (value instanceof Number) {
            return ((Number) value).doubleValue();
        }
        String str = value.toString().trim();
        if (str.isEmpty()) return 0.0;
        
        try {
            return Double.parseDouble(str);
        } catch (NumberFormatException e) {
            System.err.println("双精度浮点数转换失败: " + str + ", 使用默认值0.0");
            return 0.0;
        }
    }
    
    private Timestamp convertToTimestamp(Object value) {
        if (value instanceof Timestamp) {
            return (Timestamp) value;
        } else if (value instanceof java.util.Date) {
            return new Timestamp(((java.util.Date) value).getTime());
        } else if (value instanceof String) {
            String str = value.toString().trim();
            if (str.isEmpty()) return new Timestamp(0);
            
            try {
                return Timestamp.valueOf(str);
            } catch (Exception e) {
                System.err.println("日期转换失败: " + str + ", 使用默认值");
                return new Timestamp(0);
            }
        }
        return new Timestamp(0);
    }
    
    private boolean convertToBoolean(Object value) {
        if (value instanceof Boolean) {
            return (Boolean) value;
        } else if (value instanceof Number) {
            return ((Number) value).intValue() != 0;
        } else {
            String str = value.toString().trim().toLowerCase();
            return "true".equals(str) || "1".equals(str) || "yes".equals(str);
        }
    }
    
    private Object convertToNumber(Object value, Class<?> targetClass) {
        if (value == null) {
            return null;
        }
        
        try {
            if (value instanceof Number) {
                Number num = (Number) value;
                if (targetClass == Byte.class) {
                    return num.byteValue();
                } else if (targetClass == Short.class) {
                    return num.shortValue();
                } else if (targetClass == Integer.class) {
                    return num.intValue();
                } else if (targetClass == Long.class) {
                    return num.longValue();
                } else if (targetClass == Float.class) {
                    return num.floatValue();
                } else if (targetClass == Double.class) {
                    return num.doubleValue();
                }
            } else {
                String str = value.toString().trim();
                if (str.isEmpty()) {
                    return getDefaultValue(targetClass);
                }
                
                if (targetClass == Byte.class) {
                    return Byte.parseByte(str);
                } else if (targetClass == Short.class) {
                    return Short.parseShort(str);
                } else if (targetClass == Integer.class) {
                    return Integer.parseInt(str);
                } else if (targetClass == Long.class) {
                    return Long.parseLong(str);
                } else if (targetClass == Float.class) {
                    return Float.parseFloat(str);
                } else if (targetClass == Double.class) {
                    return Double.parseDouble(str);
                }
            }
        } catch (NumberFormatException e) {
            System.err.println("数值转换失败: " + value + " -> " + targetClass.getSimpleName() + ", 使用默认值");
            return getDefaultValue(targetClass);
        }
        
        return getDefaultValue(targetClass);
    }
    
    private Object getDefaultValue(Class<?> targetClass) {
        if (targetClass == Byte.class) return (byte) 0;
        if (targetClass == Short.class) return (short) 0;
        if (targetClass == Integer.class) return 0;
        if (targetClass == Long.class) return 0L;
        if (targetClass == Float.class) return 0.0f;
        if (targetClass == Double.class) return 0.0;
        return null;
    }
    
    public void close() {
        if (dbManager != null) {
            dbManager.close();
        }
    }
    

}