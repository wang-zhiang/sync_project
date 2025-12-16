package dim;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import java.sql.*;
import java.util.HashMap;
import java.util.Map;

public class DatabaseManager {
    private HikariDataSource sqlServerDataSource;
    private HikariDataSource clickHouseDataSource;
    
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
        System.out.println("ClickHouse连接池初始化完成");
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
    
    public void close() {
        if (sqlServerDataSource != null) {
            sqlServerDataSource.close();
        }
        if (clickHouseDataSource != null) {
            clickHouseDataSource.close();
        }
    }
}