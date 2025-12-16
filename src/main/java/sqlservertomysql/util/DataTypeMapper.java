package sqlservertomysql.util;

import java.util.HashMap;
import java.util.Map;

/**
 * SQL Server到MySQL数据类型映射工具类
 */
public class DataTypeMapper {
    private static final Map<String, String> TYPE_MAPPING = new HashMap<>();
    
    static {
        // 字符串类型
        TYPE_MAPPING.put("varchar", "VARCHAR");
        TYPE_MAPPING.put("nvarchar", "VARCHAR");
        TYPE_MAPPING.put("char", "CHAR");
        TYPE_MAPPING.put("nchar", "CHAR");
        TYPE_MAPPING.put("text", "TEXT");
        TYPE_MAPPING.put("ntext", "TEXT");
        
        // 数值类型
        TYPE_MAPPING.put("int", "INT");
        TYPE_MAPPING.put("bigint", "BIGINT");
        TYPE_MAPPING.put("smallint", "SMALLINT");
        TYPE_MAPPING.put("tinyint", "TINYINT");
        TYPE_MAPPING.put("bit", "BOOLEAN");
        TYPE_MAPPING.put("decimal", "DECIMAL");
        TYPE_MAPPING.put("numeric", "DECIMAL");
        TYPE_MAPPING.put("float", "FLOAT");
        TYPE_MAPPING.put("real", "FLOAT");
        TYPE_MAPPING.put("money", "DECIMAL(19,4)");
        TYPE_MAPPING.put("smallmoney", "DECIMAL(10,4)");
        
        // 日期时间类型
        TYPE_MAPPING.put("datetime", "DATETIME");
        TYPE_MAPPING.put("datetime2", "DATETIME");
        TYPE_MAPPING.put("smalldatetime", "DATETIME");
        TYPE_MAPPING.put("date", "DATE");
        TYPE_MAPPING.put("time", "TIME");
        TYPE_MAPPING.put("timestamp", "TIMESTAMP");
        
        // 二进制类型
        TYPE_MAPPING.put("binary", "BINARY");
        TYPE_MAPPING.put("varbinary", "VARBINARY");
        TYPE_MAPPING.put("image", "LONGBLOB");
        
        // 其他类型
        TYPE_MAPPING.put("uniqueidentifier", "VARCHAR(36)");
        TYPE_MAPPING.put("xml", "TEXT");
    }
    
    /**
     * 将SQL Server数据类型转换为MySQL数据类型
     */
    public static String mapDataType(String sqlServerType, int length, int precision, int scale) {
        String baseType = sqlServerType.toLowerCase();
        String mysqlType = TYPE_MAPPING.get(baseType);
        
        if (mysqlType == null) {
            mysqlType = "TEXT"; // 默认类型
        }
        
        // 处理长度和精度
        if (baseType.equals("varchar") || baseType.equals("nvarchar")) {
            if (length > 0 && length <= 65535) {
                mysqlType = "VARCHAR(" + length + ")";
            } else {
                mysqlType = "TEXT";
            }
        } else if (baseType.equals("char") || baseType.equals("nchar")) {
            if (length > 0 && length <= 255) {
                mysqlType = "CHAR(" + length + ")";
            }
        } else if (baseType.equals("decimal") || baseType.equals("numeric")) {
            if (precision > 0 && scale >= 0) {
                mysqlType = "DECIMAL(" + precision + "," + scale + ")";
            }
        }
        
        return mysqlType;
    }
}