package sqlserver_util.sqlservertosqlserver;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class SqlServerSchemaUtils {

    public static class ColumnInfo {
        public final String name;
        public final String dataType;
        public final Integer length;
        public final Integer precision;
        public final Integer scale;
        public final Integer datetimePrecision;
        public final boolean nullable;

        public ColumnInfo(String name, String dataType, Integer length,
                          Integer precision, Integer scale, Integer datetimePrecision, boolean nullable) {
            this.name = name;
            this.dataType = dataType;
            this.length = length;
            this.precision = precision;
            this.scale = scale;
            this.datetimePrecision = datetimePrecision;
            this.nullable = nullable;
        }
    }

    private static Integer getIntObj(ResultSet rs, String column) throws SQLException {
        Object o = rs.getObject(column);
        if (o == null) return null;
        if (o instanceof Number) return ((Number) o).intValue();
        if (o instanceof String) {
            String s = ((String) o).trim();
            if (s.isEmpty()) return null;
            try {
                return Integer.valueOf(s);
            } catch (NumberFormatException ignored) {}
        }
        int v = rs.getInt(column);
        return rs.wasNull() ? null : v;
    }

    public static List<ColumnInfo> getSourceColumns(Connection conn, String schema, String table) throws SQLException {
        String sql = "SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE, CHARACTER_MAXIMUM_LENGTH, " +
                "NUMERIC_PRECISION, NUMERIC_SCALE, DATETIME_PRECISION " +
                "FROM INFORMATION_SCHEMA.COLUMNS " +
                "WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? ORDER BY ORDINAL_POSITION";
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, schema);
            ps.setString(2, table);
            try (ResultSet rs = ps.executeQuery()) {
                List<ColumnInfo> cols = new ArrayList<>();
                while (rs.next()) {
                    String name = rs.getString("COLUMN_NAME");
                    String dt = rs.getString("DATA_TYPE");
                    String isNullable = rs.getString("IS_NULLABLE");
                    Integer length = getIntObj(rs, "CHARACTER_MAXIMUM_LENGTH");
                    Integer precision = getIntObj(rs, "NUMERIC_PRECISION");
                    Integer scale = getIntObj(rs, "NUMERIC_SCALE");
                    Integer dtp = getIntObj(rs, "DATETIME_PRECISION");
                    boolean nullable = "YES".equalsIgnoreCase(isNullable);
                    cols.add(new ColumnInfo(name, dt, length, precision, scale, dtp, nullable));
                }
                if (cols.isEmpty()) {
                    throw new IllegalArgumentException("源表不存在或无列: " + schema + "." + table);
                }
                return cols;
            }
        }
    }

    public static String buildCreateTableDDL(List<ColumnInfo> cols, String targetSchema, String targetTable) {
        String full = DbUtil.fullName(targetSchema, targetTable);
        StringBuilder sb = new StringBuilder();
        sb.append("IF OBJECT_ID(N'").append(full).append("', 'U') IS NOT NULL DROP TABLE ").append(full).append(";\n");
        sb.append("CREATE TABLE ").append(full).append(" (\n");
        for (int i = 0; i < cols.size(); i++) {
            ColumnInfo c = cols.get(i);
            sb.append("  ").append(DbUtil.q(c.name)).append(" ").append(typeDefinition(c))
                    .append(c.nullable ? " NULL" : " NOT NULL");
            if (i < cols.size() - 1) sb.append(",");
            sb.append("\n");
        }
        sb.append(");");
        return sb.toString();
    }

    private static String typeDefinition(ColumnInfo c) {
        String dt = c.dataType.toLowerCase();
        switch (dt) {
            case "varchar":
            case "nvarchar":
            case "char":
            case "nchar":
            case "varbinary":
            case "binary": {
                if (c.length == null) return dt;
                String lenStr = (c.length != null && c.length == -1) ? "MAX" : String.valueOf(c.length);
                return dt + "(" + lenStr + ")";
            }
            case "decimal":
            case "numeric": {
                int p = c.precision != null ? c.precision : 18;
                int s = c.scale != null ? c.scale : 0;
                return dt + "(" + p + "," + s + ")";
            }
            case "datetime2":
            case "datetimeoffset":
            case "time": {
                if (c.datetimePrecision != null) {
                    return dt + "(" + c.datetimePrecision + ")";
                }
                return dt;
            }
            default:
                return dt;
        }
    }

    public static boolean tableExists(Connection conn, String schema, String table) throws SQLException {
        String sql = "SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?";
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, schema);
            ps.setString(2, table);
            try (ResultSet rs = ps.executeQuery()) {
                rs.next();
                return rs.getInt(1) > 0;
            }
        }
    }

    public static String buildInsertSql(String targetSchema, String targetTable, List<ColumnInfo> cols) {
        String full = DbUtil.fullName(targetSchema, targetTable);
        String colList = cols.stream().map(c -> DbUtil.q(c.name)).collect(Collectors.joining(", "));
        String placeholders = cols.stream().map(c -> "?").collect(Collectors.joining(", "));
        return "INSERT INTO " + full + " (" + colList + ") VALUES (" + placeholders + ")";
    }

    public static String buildPagedSelectSql(String sourceSchema, String sourceTable, List<ColumnInfo> cols) {
        String full = DbUtil.fullName(sourceSchema, sourceTable);
        String colList = cols.stream().map(c -> DbUtil.q(c.name)).collect(Collectors.joining(", "));
        return "WITH src AS (" +
                "SELECT " + colList + ", ROW_NUMBER() OVER (ORDER BY (SELECT 1)) AS rn FROM " + full +
                ") SELECT " + colList + " FROM src WHERE rn BETWEEN ? AND ?";
    }

    public static long countRows(Connection conn, String schema, String table) throws SQLException {
        String sql = "SELECT COUNT(*) FROM " + DbUtil.fullName(schema, table);
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery(sql)) {
            rs.next();
            return rs.getLong(1);
        }
    }
}