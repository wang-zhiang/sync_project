package dim.dim_skuall;

import org.yaml.snakeyaml.Yaml;

import java.io.FileInputStream;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
//4.08
public class DataSyncService {
    private static final int BATCH_SIZE = 20000;
    private static final int THREAD_POOL_SIZE = 6;
    
    private DatabaseManager dbManager;
    private TaskConfig config;

    // 主方法 - 工作流程清晰可见
    public static void main(String[] args) {
        DataSyncService syncService = new DataSyncService();

        try {
            // 加载配置文件
            syncService.loadConfig("D:\\wzza\\develop\\idea_project\\ceshi\\src\\main\\java\\dim\\dim_skuall\\task.yaml");

            // 设置要同步的taskId
           // String taskId = "skuall";
            String taskId = "DabaojianSkuType";

            System.out.println("========================================");
            System.out.println("   数据同步工作脚本开始执行");
            System.out.println("   任务ID: " + taskId);
            System.out.println("   开始时间: " + getCurrentTime());
            System.out.println("========================================\n");

            TaskConfig.Task task = syncService.findTaskById(taskId);
            if (task == null) {
                System.err.println("✗ 未找到任务ID: " + taskId);
                return;
            }

            // 初始化数据库连接
            syncService.initConnections(task);

            // ============ 步骤1：清空CDC表 ============
            syncService.step1_TruncateCdcTable(task);

            // ============ 步骤2：数据同步 ============
            syncService.step2_DataSync(task);

            // ============ 步骤3：填充各节点最终表 ============
            syncService.step3_FillNodeTables(task);

            // ============ 步骤4：数据核验 ============
            syncService.step4_DataVerification(task);

            System.out.println("\n========================================");
            System.out.println("   数据同步工作脚本执行完成！");
            System.out.println("   结束时间: " + getCurrentTime());
            System.out.println("========================================");

        } catch (Exception e) {
            System.err.println("\n✗✗✗ 同步失败: " + e.getMessage());
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
        System.out.println("✓ 配置文件加载完成");
    }

    private void initConnections(TaskConfig.Task task) throws Exception {
        System.out.println("\n--- 初始化数据库连接 ---");
        dbManager.initSqlServerConnection(task.getSource());
        dbManager.initClickHouseConnection(config.getClickhouse());
        dbManager.initClickHouseNodeConnections(config.getClickhouse());
        System.out.println("✓ 所有数据库连接初始化完成\n");
    }

    private TaskConfig.Task findTaskById(String taskId) {
        return config.getTasks().stream()
                .filter(task -> taskId.equals(task.getTaskId()))
                .findFirst()
                .orElse(null);
    }

    // ============================================================
    // 步骤1：清空SQL Server CDC表
    // ============================================================
    private void step1_TruncateCdcTable(TaskConfig.Task task) throws SQLException {
        System.out.println("============================================================");
        System.out.println("步骤1：清空SQL Server CDC表");
        System.out.println("============================================================");
        System.out.println("源表: " + task.getSource().getTable());
        
        long startTime = System.currentTimeMillis();
        dbManager.truncateCdcTable(task.getSource().getTable());
        long endTime = System.currentTimeMillis();
        
        System.out.println("✓ 步骤1完成，耗时: " + (endTime - startTime) + " ms\n");
    }

    // ============================================================
    // 步骤2：数据同步（SQL Server -> ClickHouse中间表）
    // ============================================================
    private void step2_DataSync(TaskConfig.Task task) throws Exception {
        System.out.println("============================================================");
        System.out.println("步骤2：数据同步（SQL Server -> ClickHouse中间表）");
        System.out.println("============================================================");
        System.out.println("源表: " + task.getSource().getTable());
        System.out.println("目标表: " + task.getTargetTable());
        System.out.println("表类型: " + getTableTypeDesc(task.getTableType()));
        
        long startTime = System.currentTimeMillis();

        // 2.1 清空目标表
        truncateTargetTable(task);

        // 2.2 获取源表记录数
        long totalCount = dbManager.getTableCount(task.getSource().getTable());
        System.out.println("源表总记录数: " + totalCount);
        
        if (totalCount == 0) {
            System.out.println("⚠ 源表无数据，跳过同步");
            return;
        }

        // 2.3 获取字段类型信息
        String sourceTableName = task.getSource().getTable().replace("dbo.", "");
        Map<String, String> sourceColumnTypes = dbManager.getSqlServerColumnTypes(sourceTableName);
        Map<String, String> targetColumnTypes = dbManager.getClickHouseColumnTypes(task.getTargetTable());
        System.out.println("✓ 字段类型映射获取完成");

        // 2.4 执行分页同步
        performSync(task, sourceColumnTypes, targetColumnTypes, totalCount);
        
        long endTime = System.currentTimeMillis();
        System.out.println("✓ 步骤2完成，总耗时: " + (endTime - startTime) + " ms\n");
    }

    // 清空目标表（根据tableType决定清空策略）
    private void truncateTargetTable(TaskConfig.Task task) throws SQLException {
        String sql;
        String database = config.getClickhouse().getDatabase();
        
        if (task.getTableType() == 0) {
            // 分布式表：清空local表
            String localTable = task.getTargetTableLocal();
            if (localTable == null || localTable.isEmpty()) {
                System.out.println("⚠ tableType=0但未配置targetTable_local，跳过清空");
                return;
            }
            sql = String.format("TRUNCATE TABLE %s.%s ON CLUSTER %s", 
                database, localTable, task.getClusterName());
            System.out.println("清空分布式表本地表: " + localTable);
        } else if (task.getTableType() == 1) {
            // 多节点相同表：清空目标表
            sql = String.format("TRUNCATE TABLE %s.%s ON CLUSTER %s", 
                database, task.getTargetTable(), task.getClusterName());
            System.out.println("清空多节点相同表: " + task.getTargetTable());
        } else {
            // 普通表
            sql = String.format("TRUNCATE TABLE %s.%s", database, task.getTargetTable());
            System.out.println("清空普通表: " + task.getTargetTable());
        }
        
        try (Connection conn = dbManager.getClickHouseConnection();
             Statement stmt = conn.createStatement()) {
            stmt.execute(sql);
            System.out.println("✓ 目标表清空完成");
        }
    }

    // 执行分页同步
    private void performSync(TaskConfig.Task task, Map<String, String> sourceTypes, 
                           Map<String, String> targetTypes, long totalCount) throws Exception {
        
        int totalPages = (int) Math.ceil((double) totalCount / BATCH_SIZE);
        System.out.println("分页数: " + totalPages + "，每页: " + BATCH_SIZE + " 条");
        
        ExecutorService executor = Executors.newFixedThreadPool(THREAD_POOL_SIZE);
        List<CompletableFuture<Void>> futures = new ArrayList<>();
        
        for (int page = 0; page < totalPages; page++) {
            final int currentPage = page;
            CompletableFuture<Void> future = CompletableFuture.runAsync(() -> {
                try {
                    syncPage(task, currentPage, sourceTypes, targetTypes);
                } catch (Exception e) {
                    System.err.println("✗ 页面同步失败 [" + currentPage + "]: " + e.getMessage());
                    e.printStackTrace();
                }
            }, executor);
            futures.add(future);
        }
        
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
        executor.shutdown();
        
        System.out.println("✓ 所有分页同步完成");
    }

    // 同步单个分页
    private void syncPage(TaskConfig.Task task, int page, Map<String, String> sourceTypes, 
                         Map<String, String> targetTypes) throws SQLException {
        
        long startTime = System.currentTimeMillis();
        int offset = page * BATCH_SIZE;
        
        // 构建主键排序
        StringBuilder orderBy = new StringBuilder();
        for (int i = 0; i < task.getPrimaryKeys().size(); i++) {
            if (i > 0) orderBy.append(", ");
            orderBy.append(task.getPrimaryKeys().get(i));
        }
        
        // 构建源字段列表
        StringBuilder sourceFields = new StringBuilder();
        for (int i = 0; i < task.getFieldMappings().size(); i++) {
            if (i > 0) sourceFields.append(", ");
            sourceFields.append("[").append(task.getFieldMappings().get(i).getSourceField()).append("]");
        }
        
        // 分页查询SQL
        String selectSql = String.format(
            "SELECT %s FROM (" +
            "SELECT %s, ROW_NUMBER() OVER (ORDER BY %s) as rn FROM %s" +
            ") t WHERE rn > %d AND rn <= %d",
            sourceFields, sourceFields, orderBy, task.getSource().getTable(),
            offset, offset + BATCH_SIZE
        );
        
        // 构建目标字段和占位符
        StringBuilder targetFields = new StringBuilder();
        StringBuilder placeholders = new StringBuilder();
        for (int i = 0; i < task.getFieldMappings().size(); i++) {
            if (i > 0) {
                targetFields.append(", ");
                placeholders.append(", ");
            }
            targetFields.append(task.getFieldMappings().get(i).getTargetField());
            placeholders.append("?");
        }
        
        String insertSql = String.format("INSERT INTO %s (%s) VALUES (%s)",
            task.getTargetTable(), targetFields, placeholders);
        
        try (Connection sourceConn = dbManager.getSqlServerConnection();
             Connection targetConn = dbManager.getClickHouseConnection();
             PreparedStatement selectStmt = sourceConn.prepareStatement(selectSql);
             PreparedStatement insertStmt = targetConn.prepareStatement(insertSql)) {
            
            ResultSet rs = selectStmt.executeQuery();
            int batchCount = 0;
            int recordCount = 0;
            
            while (rs.next()) {
                for (int i = 0; i < task.getFieldMappings().size(); i++) {
                    TaskConfig.FieldMapping mapping = task.getFieldMappings().get(i);
                    Object value = rs.getObject(mapping.getSourceField());
                    
                    Object convertedValue = convertValue(value, 
                        sourceTypes.get(mapping.getSourceField()),
                        targetTypes.get(mapping.getTargetField()));
                    
                    insertStmt.setObject(i + 1, convertedValue);
                }
                
                insertStmt.addBatch();
                batchCount++;
                recordCount++;
                
                if (batchCount >= 1000) {
                    insertStmt.executeBatch();
                    batchCount = 0;
                }
            }
            
            if (batchCount > 0) {
                insertStmt.executeBatch();
            }
            
            long endTime = System.currentTimeMillis();
            System.out.printf("  页面 [%d] 同步完成，记录数: %d，耗时: %d ms%n", 
                page, recordCount, endTime - startTime);
        }
    }

    // ============================================================
    // 步骤3：填充各节点最终表（中间表 -> 各节点最终表）
    // ============================================================
    private void step3_FillNodeTables(TaskConfig.Task task) throws Exception {
        System.out.println("============================================================");
        System.out.println("步骤3：填充各节点最终表");
        System.out.println("============================================================");
        System.out.println("中间表: " + task.getTargetTable());
        System.out.println("最终表: " + task.getTargetTableNode());
        
        long startTime = System.currentTimeMillis();
        
        String database = config.getClickhouse().getDatabase();
        List<Connection> nodeConnections = dbManager.getAllNodeConnections();
        List<CompletableFuture<Void>> futures = new ArrayList<>();
        ExecutorService executor = Executors.newFixedThreadPool(7);
        
        // 并发处理7个节点
        for (int i = 0; i < nodeConnections.size(); i++) {
            final int nodeIndex = i;
            final Connection conn = nodeConnections.get(i);
            
            CompletableFuture<Void> future = CompletableFuture.runAsync(() -> {
                try {
                    String nodeIp = dbManager.getNodeIp(nodeIndex);
                    System.out.println("  处理节点: " + nodeIp);
                    long nodeStartTime = System.currentTimeMillis();
                    
                    // 清空节点表
                    String truncateSql = String.format("TRUNCATE TABLE %s.%s", 
                        database, task.getTargetTableNode());
                    try (Statement stmt = conn.createStatement()) {
                        stmt.setQueryTimeout(60);  // 清空操作60秒超时
                        stmt.execute(truncateSql);
                        System.out.println("  ✓ 节点 " + nodeIp + " - 清空完成");
                    }
                    
                    // 插入数据（设置更长的超时时间）
                    String insertSql = String.format("INSERT INTO %s.%s SELECT * FROM %s.%s",
                        database, task.getTargetTableNode(), database, task.getTargetTable());
                    try (Statement stmt = conn.createStatement()) {
                        stmt.setQueryTimeout(0);  // 0表示无限超时，让INSERT操作完整执行
                        System.out.println("  节点 " + nodeIp + " - 开始插入数据...");
                        stmt.execute(insertSql);
                        long nodeEndTime = System.currentTimeMillis();
                        System.out.println("  ✓ 节点 " + nodeIp + " - 数据填充完成，耗时: " + (nodeEndTime - nodeStartTime) + " ms");
                    }
                    
                } catch (SQLException e) {
                    System.err.println("  ✗ 节点 " + dbManager.getNodeIp(nodeIndex) + " 处理失败: " + e.getMessage());
                    e.printStackTrace();
                } finally {
                    try {
                        conn.close();
                    } catch (SQLException e) {
                        e.printStackTrace();
                    }
                }
            }, executor);
            
            futures.add(future);
        }
        
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
        executor.shutdown();
        
        long endTime = System.currentTimeMillis();
        System.out.println("✓ 步骤3完成，所有节点处理完毕，耗时: " + (endTime - startTime) + " ms\n");
    }

    // ============================================================
    // 步骤4：数据核验
    // ============================================================
    private void step4_DataVerification(TaskConfig.Task task) throws Exception {
        System.out.println("============================================================");
        System.out.println("步骤4：数据核验");
        System.out.println("============================================================");
        
        String database = config.getClickhouse().getDatabase();
        
        // 获取SQL Server源表数据量
        long sqlServerCount = dbManager.getTableCount(task.getSource().getTable());
        System.out.println("SQL Server 源表 [" + task.getSource().getTable() + "] 数据量: " + sqlServerCount);
        
        // 获取各节点最终表数据量
        System.out.println("\nClickHouse 各节点最终表 [" + task.getTargetTableNode() + "] 数据量:");
        List<Connection> nodeConnections = dbManager.getAllNodeConnections();
        
        for (int i = 0; i < nodeConnections.size(); i++) {
            Connection conn = nodeConnections.get(i);
            try {
                long count = dbManager.getClickHouseTableCount(conn, database, task.getTargetTableNode());
                String nodeIp = dbManager.getNodeIp(i);
                System.out.println("  节点 " + nodeIp + ": " + count + " 条");
                
                // 数据量对比
                if (count == sqlServerCount) {
                    System.out.println("    ✓ 数据量一致");
                } else {
                    System.out.println("    ⚠ 数据量不一致，差异: " + (sqlServerCount - count));
                }
            } finally {
                conn.close();
            }
        }
        
        System.out.println("\n✓ 数据核验完成\n");
    }

    // ============================================================
    // 数据类型转换方法
    // ============================================================
    private Object convertValue(Object value, String sourceType, String targetType) {
        if (value == null || "NULL".equals(String.valueOf(value).trim())) {
            return getDefaultValueForNull(targetType);
        }
        return convertByClickHouseType(value, targetType);
    }
    
    private Object getDefaultValueForNull(String targetType) {
        if (targetType == null) return null;
        
        if (targetType.startsWith("String") || targetType.startsWith("FixedString")) {
            return "";
        }
        
        if (targetType.startsWith("Int") || targetType.startsWith("UInt")) {
            if (targetType.startsWith("Int64") || targetType.startsWith("UInt64") || 
                targetType.startsWith("Int128") || targetType.startsWith("Int256")) {
                return 0L;
            }
            return 0;
        }
        
        if (targetType.startsWith("Float") || targetType.startsWith("Double") || 
            targetType.startsWith("Decimal")) {
            return 0.0;
        }
        
        if (targetType.startsWith("Date") || targetType.startsWith("DateTime")) {
            return new Timestamp(0);
        }
        
        if (targetType.startsWith("Bool")) {
            return false;
        }
        
        return null;
    }
    
    private Object convertByClickHouseType(Object value, String clickHouseType) {
        try {
            if (clickHouseType.startsWith("String") || clickHouseType.startsWith("FixedString")) {
                return value.toString();
            }
            
            if (clickHouseType.startsWith("Int8") || clickHouseType.startsWith("UInt8") ||
                clickHouseType.startsWith("Int16") || clickHouseType.startsWith("UInt16") ||
                clickHouseType.startsWith("Int32") || clickHouseType.startsWith("UInt32")) {
                return convertToInt(value);
            }
            
            if (clickHouseType.startsWith("Int64") || clickHouseType.startsWith("UInt64") ||
                clickHouseType.startsWith("Int128") || clickHouseType.startsWith("Int256")) {
                return convertToLong(value);
            }
            
            if (clickHouseType.startsWith("Float32")) {
                return convertToFloat(value);
            }
            
            if (clickHouseType.startsWith("Float64") || clickHouseType.startsWith("Double") ||
                clickHouseType.startsWith("Decimal")) {
                return convertToDouble(value);
            }
            
            if (clickHouseType.startsWith("Date") || clickHouseType.startsWith("DateTime")) {
                return convertToTimestamp(value);
            }
            
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
            if (str.contains(".")) {
                return (int) Double.parseDouble(str);
            }
            return Integer.parseInt(str);
        } catch (NumberFormatException e) {
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
            if (str.contains(".")) {
                return (long) Double.parseDouble(str);
            }
            return Long.parseLong(str);
        } catch (NumberFormatException e) {
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

    // ============================================================
    // 工具方法
    // ============================================================
    private String getTableTypeDesc(int tableType) {
        switch (tableType) {
            case 0: return "分布式表";
            case 1: return "多节点相同表";
            default: return "普通表";
        }
    }

    private static String getCurrentTime() {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        return sdf.format(new Date());
    }
    
    public void close() {
        if (dbManager != null) {
            dbManager.close();
        }
    }
}
