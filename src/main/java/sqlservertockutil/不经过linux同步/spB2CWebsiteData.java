package sqlservertockutil.不经过linux同步;

import au.com.bytecode.opencsv.CSVReader;

import java.io.FileReader;
import java.io.IOException;
import java.sql.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class spB2CWebsiteData {
    // SQL Server 连接信息
    //private static final String SQL_SERVER_URL = "jdbc:sqlserver://192.168.99.35:2766;databaseName=SearchCommentYHD";
    private static final String SQL_SERVER_URL = "jdbc:sqlserver://192.168.99.39:2800;databaseName=WebSearch";
    private static final String SQL_SERVER_USER = "sa";
    private static final String SQL_SERVER_PASSWORD = "smartpthdata";

    // ClickHouse 连接信息
    private static final String CLICKHOUSE_URL = "jdbc:clickhouse://hadoop110:8123";
    private static final String CLICKHOUSE_USER = "default";
    private static final String CLICKHOUSE_PASSWORD = "smartpath";

    // ClickHouse 表名
    private static final String CK_TABLE_NAME = "dwd.spB2CWebsiteData";

    public static void main(String[] args) {
        try {
            // 建立 SQL Server 和 ClickHouse 连接
            Connection sqlServerConn = DriverManager.getConnection(SQL_SERVER_URL, SQL_SERVER_USER, SQL_SERVER_PASSWORD);
            Connection clickHouseConn = DriverManager.getConnection(CLICKHOUSE_URL, CLICKHOUSE_USER, CLICKHOUSE_PASSWORD);

            // 获取 ClickHouse 表的字段类型
            Map<String, String> ckFieldTypes = getClickHouseTableStructure(clickHouseConn);

            // 从 SQL Server 获取表数据
            String sqlServerTableName = "trading2206";
            List<Map<String, Object>> tableData = getSqlServerTableData(sqlServerConn, sqlServerTableName);

            // 分批次插入数据到 ClickHouse 表中
            int batchSize = 3000;
            for (int i = 0; i < tableData.size(); i += batchSize) {
                int end = Math.min(i + batchSize, tableData.size());
                List<Map<String, Object>> batchData = tableData.subList(i, end);
                insertDataIntoClickHouse(clickHouseConn, batchData, ckFieldTypes, sqlServerTableName);
            }

            // 关闭连接
            sqlServerConn.close();
            clickHouseConn.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // 获取 ClickHouse 表结构
    private static Map<String, String> getClickHouseTableStructure(Connection ckConn) throws SQLException {
        Map<String, String> fieldTypes = new HashMap<>();
        String query = "DESCRIBE TABLE " + CK_TABLE_NAME;
        try (Statement stmt = ckConn.createStatement(); ResultSet rs = stmt.executeQuery(query)) {
            while (rs.next()) {
                String fieldName = rs.getString("name");
                // 跳过 ser_num 和 pt_ym 字段
                if (!fieldName.equalsIgnoreCase("ser_num") && !fieldName.equalsIgnoreCase("pt_ym")) {
                    fieldTypes.put(fieldName, rs.getString("type"));
                }
            }
        }
        System.out.println("ClickHouse table structure: " + fieldTypes);
        return fieldTypes;
    }

    // 从 SQL Server 获取表数据
    private static List<Map<String, Object>> getSqlServerTableData(Connection sqlServerConn, String tableName) throws SQLException {
        List<Map<String, Object>> tableData = new ArrayList<>();
        String query = "SELECT * FROM " + tableName;
        try (Statement stmt = sqlServerConn.createStatement(); ResultSet rs = stmt.executeQuery(query)) {
            tableData = ResultSetConverter.convertResultSetToList(rs);
        }
        return tableData;
    }

    // 将数据插入到 ClickHouse 表中
    private static void insertDataIntoClickHouse(Connection clickHouseConn, List<Map<String, Object>> tableData, Map<String, String> ckFieldTypes, String sqlServerTableName) throws SQLException {
        String ptYmValue = "20" + sqlServerTableName.substring(sqlServerTableName.length() - 4);

        for (Map<String, Object> row : tableData) {
            // 构建 ClickHouse 插入语句
            StringBuilder insertQuery = new StringBuilder("INSERT INTO " + CK_TABLE_NAME + " (ser_num, pt_ym");
            for (String ckField : ckFieldTypes.keySet()) {
                insertQuery.append(", ").append(ckField);
            }
            insertQuery.append(") VALUES (?, ?, ");
            for (int i = 0; i < ckFieldTypes.size(); i++) {
                insertQuery.append("?, ");
            }
            insertQuery.setLength(insertQuery.length() - 2); // 去掉最后的逗号
            insertQuery.append(")");

            System.out.println("Insert query: " + insertQuery);

            PreparedStatement ckStmt = clickHouseConn.prepareStatement(insertQuery.toString());

            // 填充值
            ckStmt.setString(1, "121"); // ser_num
            ckStmt.setString(2, ptYmValue); // pt_ym

            int columnIndex = 3;
            for (String ckField : ckFieldTypes.keySet()) {
                Object sqlValue = row.get(ckField.toLowerCase());
                if (sqlValue != null) {
                    System.out.println("Setting value for column " + columnIndex + ": " + sqlValue.toString() + " (type: " + ckFieldTypes.get(ckField) + ")");
                    setPreparedStatementValue(ckStmt, columnIndex, sqlValue.toString(), ckFieldTypes.get(ckField));
                } else {
                    System.out.println("No value for column " + columnIndex + ": setting default value");
                    setPreparedStatementValue(ckStmt, columnIndex, null, ckFieldTypes.get(ckField));
                }
                columnIndex++;
            }

            System.out.println("Executing insert statement with values.");
            ckStmt.executeUpdate();
            ckStmt.close();
        }
    }

    // 设置 PreparedStatement 的值
    private static void setPreparedStatementValue(PreparedStatement stmt, int index, String value, String dataType) throws SQLException {
        try {
            if (value == null) {
                // 根据数据类型设置默认值
                switch (dataType.toLowerCase()) {
                    case "int32":
                        stmt.setInt(index, 0);
                        break;
                    case "int64":
                        stmt.setLong(index, 0L);
                        break;
                    case "float32":
                        stmt.setFloat(index, 0.0f);
                        break;
                    case "float64":
                        stmt.setDouble(index, 0.0);
                        break;
                    case "boolean":
                        stmt.setBoolean(index, false);
                        break;
                    case "datetime":
                        stmt.setTimestamp(index, Timestamp.valueOf("1970-01-01 00:00:00"));
                        break;
                    case "string":
                    default:
                        stmt.setString(index, "");
                        break;
                }
            } else {
                // 设置非空值
                switch (dataType.toLowerCase()) {
                    case "int32":
                        stmt.setInt(index, Integer.parseInt(value));
                        break;
                    case "int64":
                        stmt.setLong(index, Long.parseLong(value));
                        break;
                    case "float32":
                        stmt.setFloat(index, Float.parseFloat(value));
                        break;
                    case "float64":
                        stmt.setDouble(index, Double.parseDouble(value));
                        break;
                    case "boolean":
                        stmt.setBoolean(index, Boolean.parseBoolean(value));
                        break;
                    case "datetime":
                        stmt.setTimestamp(index, convertToTimestamp(value));
                        break;
                    case "string":
                    default:
                        stmt.setString(index, value);
                        break;
                }
            }
        } catch (NumberFormatException | ParseException e) {
            // 如果转换失败，将其设置为空字符串
            System.out.println("Failed to convert value: " + value + " to type: " + dataType + ". Setting as default value.");
            setPreparedStatementValue(stmt, index, null, dataType);
        }
    }

    // 转换字符串到 Timestamp
    private static Timestamp convertToTimestamp(String value) throws ParseException {
        // 假设日期时间格式为 "yyyy-MM-dd HH:mm:ss.S"
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        return new Timestamp(dateFormat.parse(value.substring(0, 19)).getTime());
    }

    // ResultSetConverter 类定义
    static class ResultSetConverter {
        public static List<Map<String, Object>> convertResultSetToList(ResultSet rs) throws SQLException {
            List<Map<String, Object>> resultList = new ArrayList<>();
            ResultSetMetaData md = rs.getMetaData();
            int columns = md.getColumnCount();
            while (rs.next()) {
                Map<String, Object> row = new HashMap<>(columns);
                for (int i = 1; i <= columns; ++i) {
                    row.put(md.getColumnName(i).toLowerCase(), rs.getObject(i));
                }
                resultList.add(row);
            }
            return resultList;
        }
    }
}
