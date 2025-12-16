package sqlservertomysql.model;

/**
 * 列信息模型类
 */
public class ColumnInfo {
    private String columnName;
    private String dataType;
    private int maxLength;
    private int precision;
    private int scale;
    private boolean isNullable;
    private boolean isPrimaryKey;
    private boolean isIdentity;
    
    public ColumnInfo() {}
    
    public ColumnInfo(String columnName, String dataType, int maxLength, 
                     int precision, int scale, boolean isNullable, 
                     boolean isPrimaryKey, boolean isIdentity) {
        this.columnName = columnName;
        this.dataType = dataType;
        this.maxLength = maxLength;
        this.precision = precision;
        this.scale = scale;
        this.isNullable = isNullable;
        this.isPrimaryKey = isPrimaryKey;
        this.isIdentity = isIdentity;
    }
    
    // Getters and Setters
    public String getColumnName() { return columnName; }
    public void setColumnName(String columnName) { this.columnName = columnName; }
    
    public String getDataType() { return dataType; }
    public void setDataType(String dataType) { this.dataType = dataType; }
    
    public int getMaxLength() { return maxLength; }
    public void setMaxLength(int maxLength) { this.maxLength = maxLength; }
    
    public int getPrecision() { return precision; }
    public void setPrecision(int precision) { this.precision = precision; }
    
    public int getScale() { return scale; }
    public void setScale(int scale) { this.scale = scale; }
    
    public boolean isNullable() { return isNullable; }
    public void setNullable(boolean nullable) { isNullable = nullable; }
    
    public boolean isPrimaryKey() { return isPrimaryKey; }
    public void setPrimaryKey(boolean primaryKey) { isPrimaryKey = primaryKey; }
    
    public boolean isIdentity() { return isIdentity; }
    public void setIdentity(boolean identity) { isIdentity = identity; }
}