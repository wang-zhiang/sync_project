package cktosqlserverutil.cktosqlserver;

import ru.yandex.clickhouse.ClickHouseConnection;
import ru.yandex.clickhouse.ClickHouseDataSource;
import ru.yandex.clickhouse.settings.ClickHouseProperties;

import java.sql.*;
import java.util.*;

public class DatabaseManager {
    private SyncConfig config;
    private ClickHouseProperties clickHouseProps;
    
    public DatabaseManager(SyncConfig config) {
        this.config = config;
        this.clickHouseProps = new ClickHouseProperties();
        // 缩短超时时间，避免长时间挂起
        this.clickHouseProps.setSocketTimeout(60000);       // 1分钟读取超时
        this.clickHouseProps.setConnectionTimeout(10000);   // 10秒连接超时
        // this.clickHouseProps.setMaxExecutionTime(60);    // 如果不支持可注释
        // 连接池配置和查询日志在当前驱动版本中不支持，已注释
        // this.clickHouseProps.setMaxPoolSize(config.getThreadCount());
        // this.clickHouseProps.setLogQueries(true);
    }
    
    // 获取ClickHouse连接
    public ClickHouseConnection getClickHouseConnection() throws SQLException {
        ClickHouseDataSource dataSource = new ClickHouseDataSource(config.getClickHouseUrl(), clickHouseProps);
        return dataSource.getConnection(config.getClickHouseUser(), config.getClickHousePassword());
    }
    
    // 获取SQL Server连接
    public Connection getSqlServerConnection() throws SQLException {
        return DriverManager.getConnection(
            config.getSqlServerUrl(), 
            config.getSqlServerUser(), 
            config.getSqlServerPassword()
        );
    }
    
    // 获取表结构信息
    public List<ColumnInfo> getTableSchema(String tableName) throws SQLException {
        List<ColumnInfo> columns = new ArrayList<>();
        String sql = "DESCRIBE TABLE " + tableName;
        
        try (ClickHouseConnection conn = getClickHouseConnection();
             Statement stmt = conn.createStatement()) {
            // 设置查询超时
            stmt.setQueryTimeout(60); // 60秒超时
            
            try (ResultSet rs = stmt.executeQuery(sql)) {
                while (rs.next()) {
                    String name = rs.getString("name");
                    String type = rs.getString("type");
                    // ClickHouse中nullable信息在type中，如Nullable(String)
                    boolean nullable = type.toLowerCase().contains("nullable");
                    columns.add(new ColumnInfo(name, type, nullable));
                }
            }
        }
        return columns;
    }
    
    // 检查SQL Server表是否存在
    public boolean tableExists(String tableName) throws SQLException {
        try (Connection conn = getSqlServerConnection()) {
            DatabaseMetaData meta = conn.getMetaData();
            try (ResultSet rs = meta.getTables(null, null, tableName, new String[]{"TABLE"})) {
                return rs.next();
            }
        }
    }
    
    // 创建SQL Server表
    public void createTable(String tableName, List<ColumnInfo> columns) throws SQLException {
        StringBuilder sql = new StringBuilder();
        sql.append("CREATE TABLE ").append(tableName).append(" (");
        
        for (int i = 0; i < columns.size(); i++) {
            ColumnInfo col = columns.get(i);
            if (i > 0) sql.append(", ");
            sql.append("[").append(col.getName()).append("] ")
               .append(col.getSqlServerType());
            // 注释掉NOT NULL约束，让所有字段都允许为空
            // if (!col.isNullable()) {
            //     sql.append(" NOT NULL");
            // }
        }
        sql.append(")");
        
        try (Connection conn = getSqlServerConnection();
             Statement stmt = conn.createStatement()) {
            stmt.executeUpdate(sql.toString());
            System.out.println("创建表成功: " + tableName);
        }
    }
    
    // 获取总记录数 - 优化查询
    public long getTotalCount(String tableName, String whereCondition) throws SQLException {
        String sql = "SELECT COUNT(*) FROM " + tableName + " WHERE " + whereCondition;
        
        try (ClickHouseConnection conn = getClickHouseConnection();
             Statement stmt = conn.createStatement()) {
            // 设置查询超时
            stmt.setQueryTimeout(120); // 2分钟超时
            
            try (ResultSet rs = stmt.executeQuery(sql)) {
                if (rs.next()) {
                    return rs.getLong(1);
                }
                return 0;
            }
        }
    }
    
    // 检查表是否有主键或时间字段，用于优化分页
    public String getBestOrderColumn(String tableName) throws SQLException {
        String[] candidateColumns = {"id", "_id", "create_time", "createtime", "update_time", "updatetime"};
        
        try (ClickHouseConnection conn = getClickHouseConnection();
             Statement stmt = conn.createStatement()) {
            
            // 先尝试获取表结构
            String sql = "DESCRIBE TABLE " + tableName;
            try (ResultSet rs = stmt.executeQuery(sql)) {
                Set<String> columns = new HashSet<>();
                while (rs.next()) {
                    columns.add(rs.getString("name").toLowerCase());
                }
                
                // 按优先级查找合适的排序字段
                for (String candidate : candidateColumns) {
                    if (columns.contains(candidate.toLowerCase())) {
                        return candidate;
                    }
                }
            }
        }
        
        return null; // 没有找到合适的排序字段
    }
    
    // 获取唯一列的最小值和最大值，用于分页范围计算
    public Object[] getUniqueColumnRange(String tableName, String uniqueColumn, String whereCondition) throws SQLException {
        String sql = String.format(
            "SELECT MIN(%s) as min_val, MAX(%s) as max_val FROM %s WHERE %s",
            uniqueColumn, uniqueColumn, tableName, whereCondition
        );
        
        try (ClickHouseConnection conn = getClickHouseConnection();
             Statement stmt = conn.createStatement()) {
            
            stmt.setQueryTimeout(120);
            try (ResultSet rs = stmt.executeQuery(sql)) {
                if (rs.next()) {
                    return new Object[]{rs.getObject("min_val"), rs.getObject("max_val")};
                }
            }
        }
        return new Object[]{null, null};
    }
    
    // 基于唯一列值获取下一批数据
    public List<Map<String, Object>> getDataByUniqueColumn(String tableName, String uniqueColumn, 
                                                          String whereCondition, List<ColumnInfo> columns,
                                                          Object lastValue, int pageSize) throws SQLException {
        List<Map<String, Object>> data = new ArrayList<>();
        
        String sql;
        if (lastValue == null) {
            // 第一页
            sql = String.format(
                "SELECT * FROM %s WHERE %s ORDER BY %s LIMIT %d",
                tableName, whereCondition, uniqueColumn, pageSize
            );
        } else {
            // 后续页面，使用WHERE条件过滤
            sql = String.format(
                "SELECT * FROM %s WHERE %s AND %s > ? ORDER BY %s LIMIT %d",
                tableName, whereCondition, uniqueColumn, uniqueColumn, pageSize
            );
        }
        
        try (ClickHouseConnection conn = getClickHouseConnection();
             PreparedStatement pstmt = conn.prepareStatement(sql)) {
            
            pstmt.setQueryTimeout(300);
            
            if (lastValue != null) {
                pstmt.setObject(1, lastValue);
            }
            
            try (ResultSet rs = pstmt.executeQuery()) {
                while (rs.next()) {
                    Map<String, Object> row = new HashMap<>();
                    for (ColumnInfo column : columns) {
                        Object value = rs.getObject(column.getName());
                        row.put(column.getName(), value);
                    }
                    data.add(row);
                }
            }
        }
        
        return data;
    }
    
    // 添加ROW_NUMBER分页查询方法
    public List<Map<String, Object>> getDataByRowNumber(String tableName, String whereCondition,
                                                       List<ColumnInfo> columns, long offset, int pageSize) throws SQLException {
        List<Map<String, Object>> data = new ArrayList<>();
        
        String sql = String.format(
            "SELECT * FROM (SELECT *, ROW_NUMBER() OVER (ORDER BY (SELECT 1)) as rn FROM %s WHERE %s) t WHERE rn > %d AND rn <= %d",
            tableName, whereCondition, offset, offset + pageSize
        );
        
        try (ClickHouseConnection conn = getClickHouseConnection();
             Statement stmt = conn.createStatement()) {
            
            stmt.setQueryTimeout(300);
            
            try (ResultSet rs = stmt.executeQuery(sql)) {
                while (rs.next()) {
                    Map<String, Object> row = new HashMap<>();
                    for (ColumnInfo column : columns) {
                        Object value = rs.getObject(column.getName());
                        row.put(column.getName(), value);
                    }
                    data.add(row);
                }
            }
        }
        
        return data;
    }
} // 添加这个大括号来结束类定义