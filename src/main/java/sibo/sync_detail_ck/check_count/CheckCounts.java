package sibo.sync_detail_ck.check_count;

import sibo.sync_detail_ck.SyncConfig;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.sql.ResultSet;
import java.sql.SQLException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.HashMap;

public class CheckCounts {

    // MySQL 配置源
    private static final String MYSQL_URL = "jdbc:mysql://192.168.3.138:3306/smartpath_admin?useSSL=false&serverTimezone=UTC&characterEncoding=utf8";
    private static final String MYSQL_USER = "root";
    private static final String MYSQL_PASSWORD = "smartpthdata";
    private static final String MYSQL_CONFIG_TABLE = "ceshi_251110";
    // 可选：只检查部分配置行（支持完整 WHERE ... 或仅条件）
    private static final String MYSQL_CONFIG_WHERE = "Channel =  '京东'   "; // 例："Channel='天猫' AND IndustryId IN (1,6)"

    // ClickHouse 目标
    private static final String CLICKHOUSE_URL = "jdbc:clickhouse://hadoop110:8123/test";
    private static final String CLICKHOUSE_USER = "default";
    private static final String CLICKHOUSE_PASSWORD = "smartpath";
    private static final String TARGET_TABLE = "test.ceshi_1110";

    // 选择要检查的源表集合（0=原表，1=ty，2=hy）
    private static final List<Integer> SYNC_TABLE_SELECTION = Arrays.asList(1,2);

    // 与同步一致的 SQL Server WHERE 过滤（默认索引友好写法）
    private static final boolean USE_INDEX_FRIENDLY_WHERE = true;
    private static final String WHERE_INDEX_FRIENDLY = "AddDate >= '2024-01-01' AND AddDate < '2025-10-01'";
    private static final String WHERE_FUNCTION_MODE = "LEFT(CONVERT(VARCHAR(8), AddDate, 112), 6) >= '202401' AND LEFT(CONVERT(VARCHAR(8), AddDate, 112), 6) <= '202510'";
    private static String sqlServerWhereFilter() {
        return USE_INDEX_FRIENDLY_WHERE ? WHERE_INDEX_FRIENDLY : WHERE_FUNCTION_MODE;
    }

    public static void main(String[] args) {
        System.out.println("开始检查数据量...");
        tryLoadClassQuietly("com.mysql.cj.jdbc.Driver");

        List<SyncConfig> configs;
        try {
            configs = loadConfigsFromMySQL();
        } catch (Exception ex) {
            System.err.println("读取配置失败: " + ex.getMessage());
            return;
        }

        int total = configs.size();
        System.out.printf("读取到 %d 个同步配置%n", total);
        System.out.println("使用配置过滤条件: " + (MYSQL_CONFIG_WHERE == null || MYSQL_CONFIG_WHERE.trim().isEmpty() ? "<无>" : MYSQL_CONFIG_WHERE));
        System.out.println("SQL Server WHERE 过滤: " + sqlServerWhereFilter());
        System.out.println("检查表选择: " + SYNC_TABLE_SELECTION);

        if (total == 0) {
            System.out.println("无配置可检查，任务结束。");
            return;
        }

        int equal = 0;
        int notEqual = 0;
        int failed = 0;

        List<String> missingBase = new ArrayList<>();
        List<String> missingTy = new ArrayList<>();
        List<String> missingHy = new ArrayList<>();

        for (int i = 0; i < total; i++) {
            SyncConfig cfg = configs.get(i);
            int idx = i + 1;

            SqlServerConnInfo ssInfo = parseSqlServerConnection(cfg.getSqlServerConnection());
            SchemaTable stBase = extractSchemaAndTable(cfg.getTableName());
            String schema = stBase.schema != null ? stBase.schema : (ssInfo.schema != null ? ssInfo.schema : "dbo");
            String basePure = stBase.table;

            ClickHouseConnInfo ckInfo = parseClickHouseUrl(CLICKHOUSE_URL);
            System.out.printf("[%d/%d] 检查: Channel=%s, IndustryId=%d, baseTable=%s%n", idx, total, cfg.getChannel(), cfg.getIndustryId(), cfg.getTableName());
            System.out.printf("[%d/%d] 源SQLServer: %s:%d/%s.%s；目标ClickHouse: %s:%d/%s，目标表=%s；WHERE过滤: %s%n",
                    idx, total,
                    ssInfo.host, ssInfo.port, ssInfo.database, schema,
                    ckInfo.host, ckInfo.port, ckInfo.database, TARGET_TABLE,
                    sqlServerWhereFilter());

            List<String> presentTables = new ArrayList<>();
            try (Connection conn = openSqlServerConnection(ssInfo)) {
                for (Integer sel : SYNC_TABLE_SELECTION) {
                    if (sel == null) continue;
                    String candidatePure;
                    String mark;
                    if (sel == 0) { candidatePure = basePure; mark = "BASE"; }
                    else if (sel == 1) { candidatePure = basePure + "ty"; mark = "TY"; }
                    else if (sel == 2) { candidatePure = basePure + "hy"; mark = "HY"; }
                    else { throw new IllegalArgumentException("非法选择值: " + sel + "，有效值为 0(原表),1(ty),2(hy)"); }

                    if (tableExists(conn, schema, candidatePure)) {
                        presentTables.add(stBase.schema != null ? stBase.schema + "." + candidatePure : candidatePure);
                    } else {
                        String idstr = String.format("%s.%s (Channel=%s, IndustryId=%d)", schema, candidatePure, cfg.getChannel(), cfg.getIndustryId());
                        if (sel == 0) missingBase.add(idstr);
                        else if (sel == 1) missingTy.add(idstr);
                        else missingHy.add(idstr);
                        System.out.printf("[%d/%d] 源%s表不存在，跳过: %s%n", idx, total, mark, idstr);
                    }
                }
            } catch (Exception ex) {
                System.err.printf("[%d/%d] 连接SQL Server失败，跳过该配置: %s%n", idx, total, ex.getMessage());
                // 不中断整体流程，计失败并继续后续项
                failed++;
                continue;
            }

            if (presentTables.isEmpty()) {
                System.out.printf("[%d/%d] 该配置选择的表均不存在，跳过：Channel=%s, baseTable=%s%n", idx, total, cfg.getChannel(), cfg.getTableName());
                // 不计失败，继续下一条
                continue;
            }

            try {
                long sqlSum = sumSqlServerCounts(cfg.getSqlServerConnection(), schema, presentTables, sqlServerWhereFilter());
                long ckSum = sumClickHouseCounts(TARGET_TABLE, cfg.getChannel(), presentTables);
                String mark = (sqlSum == ckSum) ? "✓" : "✗";
                System.out.printf("[%d/%d] SQL Server(存在表总和): %d 条, ClickHouse(存在表总和): %d 条 %s%n", idx, total, sqlSum, ckSum, mark);
                if (sqlSum == ckSum) equal++; else notEqual++;
            } catch (Exception ex) {
                failed++;
                System.err.printf("[%d/%d] 检查失败: %s%n", idx, total, ex.getMessage());
            }
        }

        System.out.println("缺失 BASE 表列表：");
        if (missingBase.isEmpty()) System.out.println(" - <无>");
        else for (String s : missingBase) System.out.println(" - " + s);

        System.out.println("缺失 TY 表列表：");
        if (missingTy.isEmpty()) System.out.println(" - <无>");
        else for (String s : missingTy) System.out.println(" - " + s);

        System.out.println("缺失 HY 表列表：");
        if (missingHy.isEmpty()) System.out.println(" - <无>");
        else for (String s : missingHy) System.out.println(" - " + s);

        System.out.printf("所有检查完成！一致: %d 个，不一致: %d 个，失败: %d 个%n", equal, notEqual, failed);
    }

    private static List<SyncConfig> loadConfigsFromMySQL() throws SQLException {
        String baseSql = "SELECT Channel, IndustryId, sqlServerConnection, tableName FROM " + MYSQL_CONFIG_TABLE;
        String whereClause = MYSQL_CONFIG_WHERE == null ? "" : MYSQL_CONFIG_WHERE.trim();
        if (!whereClause.isEmpty() && !whereClause.toLowerCase(Locale.ENGLISH).startsWith("where ")) {
            whereClause = "WHERE " + whereClause;
        }
        String sql = baseSql + (whereClause.isEmpty() ? "" : " " + whereClause);
        System.out.println("使用配置过滤条件: " + (whereClause.isEmpty() ? "<无>" : whereClause));

        try (Connection conn = DriverManager.getConnection(MYSQL_URL, MYSQL_USER, MYSQL_PASSWORD);
             Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery(sql)) {
            List<SyncConfig> configs = new ArrayList<>();
            while (rs.next()) {
                configs.add(SyncConfig.fromResultSet(rs));
            }
            return configs;
        }
    }

    private static boolean tableExists(Connection conn, String schema, String pureTable) throws SQLException {
        String sql = "SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?";
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, schema);
            ps.setString(2, pureTable);
            try (ResultSet rs = ps.executeQuery()) {
                rs.next();
                return rs.getInt(1) > 0;
            }
        }
    }

    private static long sumSqlServerCounts(String sqlServerConnStr, String schema, List<String> tableNames, String where) throws SQLException {
        SqlServerConnInfo ss = parseSqlServerConnection(sqlServerConnStr);
        try (Connection conn = openSqlServerConnection(ss)) {
            long sum = 0;
            for (String t : tableNames) {
                SchemaTable st = extractSchemaAndTable(t);
                String s = st.schema != null ? st.schema : schema;
                String qualified = "[" + s + "].[" + st.table + "]";
                sum += querySqlServerCountWithFilter(conn, qualified, where);
            }
            return sum;
        }
    }

    private static long sumClickHouseCounts(String targetTable, String channel, List<String> tableNames) throws SQLException {
        try (Connection ck = openClickHouseConnection()) {
            String placeholders = String.join(",", Collections.nCopies(tableNames.size(), "?"));
            String sql = "SELECT COUNT(*) FROM " + targetTable + " WHERE Channel=? AND tableName IN (" + placeholders + ")";
            try (PreparedStatement ps = ck.prepareStatement(sql)) {
                ps.setString(1, channel);
                for (int i = 0; i < tableNames.size(); i++) {
                    ps.setString(i + 2, tableNames.get(i));
                }
                try (ResultSet rs = ps.executeQuery()) {
                    rs.next();
                    return rs.getLong(1);
                }
            }
        }
    }

    private static long querySqlServerCountWithFilter(Connection conn, String qualifiedTable, String where) throws SQLException {
        String sql = "SELECT COUNT(*) FROM " + qualifiedTable + (where != null && !where.trim().isEmpty() ? " WHERE " + where : "");
        System.out.println("SQL Server 检查计数SQL: " + sql);
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery(sql)) {
            rs.next();
            return rs.getLong(1);
        }
    }

    private static Connection openClickHouseConnection() throws SQLException {
        tryLoadClassQuietly("com.clickhouse.jdbc.ClickHouseDriver");
        tryLoadClassQuietly("ru.yandex.clickhouse.ClickHouseDriver");
        tryLoadClassQuietly("com.github.housepower.jdbc.ClickHouseDriver");
        java.util.Properties props = new java.util.Properties();
        props.setProperty("user", CLICKHOUSE_USER);
        props.setProperty("password", CLICKHOUSE_PASSWORD);
        return DriverManager.getConnection(CLICKHOUSE_URL, props);
    }

    private static Connection openSqlServerConnection(SqlServerConnInfo info) throws SQLException {
        tryLoadClassQuietly("com.microsoft.sqlserver.jdbc.SQLServerDriver");
        String url = "jdbc:sqlserver://" + info.host + ":" + info.port +
                ";databaseName=" + info.database +
                ";encrypt=false;trustServerCertificate=true;loginTimeout=30";
        java.util.Properties props = new java.util.Properties();
        props.setProperty("user", info.user);
        props.setProperty("password", info.password);
        return DriverManager.getConnection(url, props);
    }

    private static void tryLoadClassQuietly(String className) {
        try { Class.forName(className); } catch (Throwable ignored) {}
    }

    private static String firstNonNull(String... vals) {
        for (String v : vals) {
            if (v != null && !v.isEmpty()) return v;
        }
        return null;
    }

    private static SqlServerConnInfo parseSqlServerConnection(String connStr) {
        if (connStr == null || connStr.isEmpty()) {
            throw new IllegalArgumentException("sqlServerConnection 为空");
        }
        Map<String, String> kv = new HashMap<>();
        for (String part : connStr.split(";")) {
            part = part.trim();
            if (part.isEmpty()) continue;
            int idx = part.indexOf('=');
            if (idx <= 0) continue;
            kv.put(part.substring(0, idx).trim().toLowerCase(Locale.ENGLISH), part.substring(idx + 1).trim());
        }

        String host = null, database = null, user = null, password = null, schema = null;
        int port = 1433;

        String ds = firstNonNull(kv.get("data source"), kv.get("server"));
        if (ds != null) {
            if (ds.contains(",")) {
                String[] hp = ds.split(",", 2);
                host = hp[0].trim();
                try { port = Integer.parseInt(hp[1].trim()); } catch (Exception ignored) { port = 1433; }
            } else if (ds.contains("\\")) {
                host = ds.split("\\\\", 2)[0].trim();
            } else {
                host = ds.trim();
            }
        }

        database = firstNonNull(kv.get("initial catalog"), kv.get("database"), kv.get("database name"));
        user = firstNonNull(kv.get("user id"), kv.get("uid"), kv.get("user"));
        password = firstNonNull(kv.get("password"), kv.get("pwd"));
        schema = kv.getOrDefault("schema", "dbo");

        if (host == null || database == null || user == null || password == null) {
            throw new IllegalArgumentException("SQL Server 连接字符串不完整，需包含 Data Source/Server, Initial Catalog/Database, User Id, Password");
        }

        SqlServerConnInfo info = new SqlServerConnInfo();
        info.host = host;
        info.port = port;
        info.database = database;
        info.user = user;
        info.password = password;
        info.schema = schema;
        return info;
    }

    private static SchemaTable extractSchemaAndTable(String tableName) {
        if (tableName == null) throw new IllegalArgumentException("tableName 不能为空");
        String name = tableName.trim();
        if (name.contains(".")) {
            String[] parts = name.split("\\.", 2);
            return new SchemaTable(parts[0], parts[1]);
        }
        return new SchemaTable(null, name);
    }

    private static ClickHouseConnInfo parseClickHouseUrl(String url) {
        ClickHouseConnInfo info = new ClickHouseConnInfo();
        if (url == null || url.isEmpty()) return info;

        String s = url.trim();
        if (s.startsWith("jdbc:")) s = s.substring(5);
        int schemeIdx = s.indexOf("://");
        String rest = schemeIdx >= 0 ? s.substring(schemeIdx + 3) : s;

        int slash = rest.indexOf('/');
        String hostPort = slash >= 0 ? rest.substring(0, slash) : rest;
        String dbAndParams = slash >= 0 ? rest.substring(slash + 1) : "";

        int colon = hostPort.indexOf(':');
        if (colon >= 0) {
            info.host = hostPort.substring(0, colon);
            try { info.port = Integer.parseInt(hostPort.substring(colon + 1)); } catch (NumberFormatException ignored) { info.port = 8123; }
        } else {
            info.host = hostPort;
            info.port = 8123;
        }

        String db = dbAndParams;
        int q = db.indexOf('?');
        if (q >= 0) db = db.substring(0, q);
        info.database = db.isEmpty() ? "default" : db;

        return info;
    }

    private static class SqlServerConnInfo {
        String host;
        int port = 1433;
        String database;
        String user;
        String password;
        String schema = "dbo";
    }

    private static class SchemaTable {
        final String schema;
        final String table;
        SchemaTable(String s, String t) { this.schema = s; this.table = t; }
    }

    private static class ClickHouseConnInfo {
        String host = "unknown";
        int port = 8123;
        String database = "default";
    }
}