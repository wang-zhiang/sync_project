package dim.dim_skuall;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import java.util.List;
import java.util.ArrayList;

public class DatabaseManager {
    private HikariDataSource sqlServerDataSource;
    private HikariDataSource clickHouseDataSource;
    private List<HikariDataSource> clickHouseNodeDataSources;
    
    public void initSqlServerConnection(TaskConfig.Source source) {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(String.format("jdbc:sqlserver://%s:%d;databaseName=%s;encrypt=false", 
            source.getHost(), source.getPort(), source.getDatabase()));
        config.setUsername(source.getUser());
        config.setPassword(source.getPassword());
        config.setMaximumPoolSize(10);
        config.setMinimumIdle(2);
        
        sqlServerDataSource = new HikariDataSource(config);
        System.out.println("SQL Server连接池初始化完成");
    }
    
    public void initClickHouseConnection(TaskConfig.ClickHouseConfig ckConfig) {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(String.format("jdbc:clickhouse://%s:%d/%s", 
            ckConfig.getHost(), ckConfig.getPort(), ckConfig.getDatabase()));
        config.setUsername(ckConfig.getUsername());
        config.setPassword(ckConfig.getPassword());
        config.setMaximumPoolSize(10);
        config.setMinimumIdle(2);
        
        clickHouseDataSource = new HikariDataSource(config);
        System.out.println("ClickHouse连接池初始化完成（主节点：" + ckConfig.getHost() + "）");
    }
    
    // 初始化所有ClickHouse节点连接（104-110共7个节点）
    public void initClickHouseNodeConnections(TaskConfig.ClickHouseConfig ckConfig) {
        clickHouseNodeDataSources = new ArrayList<>();
        String[] nodeIps = {"192.168.5.104", "192.168.5.105", "192.168.5.106", 
                           "192.168.5.107", "192.168.5.108", "192.168.5.109", "192.168.5.110"};
        
        for (String ip : nodeIps) {
            HikariConfig config = new HikariConfig();
            // 增加超时参数，socket_timeout=600秒，connection_timeout=60秒
            config.setJdbcUrl(String.format("jdbc:clickhouse://%s:%d/%s?socket_timeout=600000&connection_timeout=60000", 
                ip, ckConfig.getPort(), ckConfig.getDatabase()));
            config.setUsername(ckConfig.getUsername());
            config.setPassword(ckConfig.getPassword());
            config.setMaximumPoolSize(5);
            config.setMinimumIdle(1);
            config.setConnectionTimeout(60000);  // 连接超时60秒
            config.setMaxLifetime(1800000);      // 连接最大生命周期30分钟
            
            HikariDataSource dataSource = new HikariDataSource(config);
            clickHouseNodeDataSources.add(dataSource);
            System.out.println("ClickHouse节点连接池初始化完成：" + ip);
        }
    }
    
    public List<Connection> getAllNodeConnections() throws SQLException {
        List<Connection> connections = new ArrayList<>();
        for (HikariDataSource ds : clickHouseNodeDataSources) {
            connections.add(ds.getConnection());
        }
        return connections;
    }
    
    public String getNodeIp(int nodeIndex) {
        String[] nodeIps = {"192.168.5.104", "192.168.5.105", "192.168.5.106", 
                           "192.168.5.107", "192.168.5.108", "192.168.5.109", "192.168.5.110"};
        return nodeIndex >= 0 && nodeIndex < nodeIps.length ? nodeIps[nodeIndex] : "unknown";
    }
    
    public Connection getSqlServerConnection() throws SQLException {
        return sqlServerDataSource.getConnection();
    }
    
    public Connection getClickHouseConnection() throws SQLException {
        return clickHouseDataSource.getConnection();
    }
    
    // 获取SQL Server表的字段类型信息
    public Map<String, String> getSqlServerColumnTypes(String tableName) throws SQLException {
        Map<String, String> columnTypes = new HashMap<>();
        String sql = "SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = ?";
        
        try (Connection conn = getSqlServerConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setString(1, tableName.replace("dbo.", ""));
            ResultSet rs = stmt.executeQuery();
            
            while (rs.next()) {
                columnTypes.put(rs.getString("COLUMN_NAME"), rs.getString("DATA_TYPE"));
            }
        }
        return columnTypes;
    }
    
    // 获取ClickHouse表的字段类型信息
    public Map<String, String> getClickHouseColumnTypes(String tableName) throws SQLException {
        Map<String, String> columnTypes = new HashMap<>();
        String sql = "SELECT name, type FROM system.columns WHERE table = ? AND database = ?";
        
        try (Connection conn = getClickHouseConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setString(1, tableName);
            stmt.setString(2, clickHouseDataSource.getJdbcUrl().split("/")[3]);
            ResultSet rs = stmt.executeQuery();
            
            while (rs.next()) {
                columnTypes.put(rs.getString("name"), rs.getString("type"));
            }
        }
        return columnTypes;
    }
    
    // 清空SQL Server CDC表
    public void truncateCdcTable(String sourceTableName) throws SQLException {
        // 生成CDC表名：cdc.dbo_表名_CT
        String tableName = sourceTableName.replace("dbo.", "");
        String cdcTableName = "cdc.dbo_" + tableName + "_CT";
        String sql = "TRUNCATE TABLE " + cdcTableName;
        
        try (Connection conn = getSqlServerConnection();
             Statement stmt = conn.createStatement()) {
            stmt.execute(sql);
            System.out.println("✓ 已清空CDC表：" + cdcTableName);
        } catch (SQLException e) {
            System.err.println("✗ 清空CDC表失败：" + cdcTableName + " - " + e.getMessage());
            throw e;
        }
    }
    
    // 获取SQL Server表的数据量
    public long getTableCount(String tableName) throws SQLException {
        String sql = "SELECT COUNT(*) FROM " + tableName;
        try (Connection conn = getSqlServerConnection();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            return rs.next() ? rs.getLong(1) : 0;
        }
    }
    
    // 获取ClickHouse表的数据量（指定节点连接）
    public long getClickHouseTableCount(Connection conn, String database, String tableName) throws SQLException {
        String sql = "SELECT COUNT(*) FROM " + database + "." + tableName;
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            return rs.next() ? rs.getLong(1) : 0;
        }
    }
    
    public void close() {
        if (sqlServerDataSource != null) {
            sqlServerDataSource.close();
        }
        if (clickHouseDataSource != null) {
            clickHouseDataSource.close();
        }
        if (clickHouseNodeDataSources != null) {
            for (HikariDataSource ds : clickHouseNodeDataSources) {
                if (ds != null) {
                    ds.close();
                }
            }
        }
    }
}