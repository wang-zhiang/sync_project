package sqlservertomysql.service;

import sqlservertomysql.model.ColumnInfo;
import sqlservertomysql.util.DataTypeMapper;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * 表结构服务类
 */
public class TableStructureService {
    
    /**
     * 获取SQL Server表结构信息
     */
    public List<ColumnInfo> getSqlServerTableStructure(Connection conn, String tableName) throws SQLException {
        List<ColumnInfo> columns = new ArrayList<>();
        
        String sql = "SELECT " +
                "c.COLUMN_NAME, " +
                "c.DATA_TYPE, " +
                "ISNULL(c.CHARACTER_MAXIMUM_LENGTH, 0) as MAX_LENGTH, " +
                "ISNULL(c.NUMERIC_PRECISION, 0) as PRECISION, " +
                "ISNULL(c.NUMERIC_SCALE, 0) as SCALE, " +
                "CASE WHEN c.IS_NULLABLE = 'YES' THEN 1 ELSE 0 END as IS_NULLABLE, " +
                "CASE WHEN pk.COLUMN_NAME IS NOT NULL THEN 1 ELSE 0 END as IS_PRIMARY_KEY, " +
                "CASE WHEN COLUMNPROPERTY(OBJECT_ID(c.TABLE_SCHEMA + '.' + c.TABLE_NAME), c.COLUMN_NAME, 'IsIdentity') = 1 THEN 1 ELSE 0 END as IS_IDENTITY " +
                "FROM INFORMATION_SCHEMA.COLUMNS c " +
                "LEFT JOIN ( " +
                "    SELECT ku.TABLE_NAME, ku.COLUMN_NAME " +
                "    FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc " +
                "    INNER JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE ku " +
                "        ON tc.CONSTRAINT_NAME = ku.CONSTRAINT_NAME " +
                "    WHERE tc.CONSTRAINT_TYPE = 'PRIMARY KEY' " +
                ") pk ON c.TABLE_NAME = pk.TABLE_NAME AND c.COLUMN_NAME = pk.COLUMN_NAME " +
                "WHERE c.TABLE_NAME = ? " +
                "ORDER BY c.ORDINAL_POSITION";
        
        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setString(1, tableName);
            try (ResultSet rs = pstmt.executeQuery()) {
                while (rs.next()) {
                    ColumnInfo column = new ColumnInfo(
                        rs.getString("COLUMN_NAME"),
                        rs.getString("DATA_TYPE"),
                        rs.getInt("MAX_LENGTH"),
                        rs.getInt("PRECISION"),
                        rs.getInt("SCALE"),
                        rs.getBoolean("IS_NULLABLE"),
                        rs.getBoolean("IS_PRIMARY_KEY"),
                        rs.getBoolean("IS_IDENTITY")
                    );
                    columns.add(column);
                }
            }
        }
        
        return columns;
    }
    
    /**
     * 在MySQL中创建表
     */
    public void createMySQLTable(Connection conn, String tableName, List<ColumnInfo> columns) throws SQLException {
        StringBuilder sql = new StringBuilder();
        sql.append("CREATE TABLE IF NOT EXISTS `").append(tableName).append("` (\n");
        
        List<String> primaryKeys = new ArrayList<>();
        
        for (int i = 0; i < columns.size(); i++) {
            ColumnInfo column = columns.get(i);
            sql.append("  `").append(column.getColumnName()).append("` ");
            
            // 数据类型映射
            String mysqlType = DataTypeMapper.mapDataType(
                column.getDataType(), 
                column.getMaxLength(), 
                column.getPrecision(), 
                column.getScale()
            );
            sql.append(mysqlType);
            
            // 自增列：只有当列既是自增列又是主键时才设置AUTO_INCREMENT
            if (column.isIdentity() && column.isPrimaryKey()) {
                sql.append(" AUTO_INCREMENT");
            }
            
            // 是否允许NULL
            // 注释掉或删除NOT NULL判断
            // if (!column.isNullable()) {
            //     sql.append(" NOT NULL");
            // }
            
            // 主键收集
            if (column.isPrimaryKey()) {
                primaryKeys.add(column.getColumnName());
            }
            
            if (i < columns.size() - 1) {
                sql.append(",\n");
            }
        }
        
        // 添加主键约束
        if (!primaryKeys.isEmpty()) {
            sql.append(",\n  PRIMARY KEY (");
            for (int i = 0; i < primaryKeys.size(); i++) {
                sql.append("`").append(primaryKeys.get(i)).append("`");
                if (i < primaryKeys.size() - 1) {
                    sql.append(", ");
                }
            }
            sql.append(")");
        }
        
        sql.append("\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4");
        
        try (Statement stmt = conn.createStatement()) {
            stmt.execute(sql.toString());
        }
    }
    
    /**
     * 检查MySQL表是否存在
     */
    public boolean tableExists(Connection conn, String tableName) throws SQLException {
        String sql = "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = DATABASE() AND table_name = ?";
        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setString(1, tableName);
            try (ResultSet rs = pstmt.executeQuery()) {
                if (rs.next()) {
                    return rs.getInt(1) > 0;
                }
            }
        }
        return false;
    }
}