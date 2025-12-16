package cktosqlserverutil.cktosqlserver;

public class ColumnInfo {
    private String name;
    private String clickHouseType;
    private String sqlServerType;
    private boolean nullable;
    
    public ColumnInfo(String name, String clickHouseType, boolean nullable) {
        this.name = name;
        this.clickHouseType = clickHouseType;
        this.nullable = nullable;
        this.sqlServerType = mapToSqlServerType(clickHouseType);
    }
    
    private String mapToSqlServerType(String chType) {
        chType = chType.toLowerCase();
        if (chType.contains("string") || chType.contains("fixedstring")) {
            return "NVARCHAR(MAX)";
        } else if (chType.contains("int8")) {
            return "TINYINT";
        } else if (chType.contains("int16")) {
            return "SMALLINT";
        } else if (chType.contains("int32")) {
            return "INT";
        } else if (chType.contains("int64")) {
            return "BIGINT";
        } else if (chType.contains("float32")) {
            return "REAL";
        } else if (chType.contains("float64")) {
            return "FLOAT";
        } else if (chType.contains("decimal")) {
            return "DECIMAL(18,2)";
        } else if (chType.contains("date")) {
            return "DATE";
        } else if (chType.contains("datetime")) {
            return "DATETIME";
        } else {
            return "NVARCHAR(MAX)"; // 默认类型
        }
    }
    
    // Getters
    public String getName() { return name; }
    public String getClickHouseType() { return clickHouseType; }
    public String getSqlServerType() { return sqlServerType; }
    public boolean isNullable() { return nullable; }
}