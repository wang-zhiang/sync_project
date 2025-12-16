package sibo.sync_detail_ck;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.*;

public class SqlServerToClickHouseSyncer {

    // MySQL 配置源
    private static final String MYSQL_URL = "jdbc:mysql://192.168.3.138:3306/smartpath_admin?useSSL=false&serverTimezone=UTC&characterEncoding=utf8";
    private static final String MYSQL_USER = "root";
    private static final String MYSQL_PASSWORD = "smartpthdata";
    private static final String MYSQL_CONFIG_TABLE = "ceshi_251110";
    // 新增：可选 WHERE 过滤条件（留空表示不过滤）  mysql配置表的条件
    private static final String MYSQL_CONFIG_WHERE = "Channel =  '天猫' and IndustryId in (28)  "; // 示例: "WHERE Channel='天猫' AND IndustryId IN (1,6)"

    // ClickHouse 目标
    private static final String CLICKHOUSE_URL = "jdbc:clickhouse://hadoop110:8123/test";
    private static final String CLICKHOUSE_USER = "default";
    private static final String CLICKHOUSE_PASSWORD = "smartpath";
    private static final String TARGET_TABLE = "test.ceshi_1110"; // 目标表

    // 同步参数与过滤
    private static final int PAGE_SIZE = 5000;
    private static final int THREADS = 5; // 单表分页并发数
    private static final String ORDER_BY_COLUMNS = "Id"; // 排序字段（可改为 "AddDate, Id"）

    // 新增：选择要同步的源表集合（0=原表，1=ty，2=hy）
    private static final java.util.List<Integer> SYNC_TABLE_SELECTION = java.util.Arrays.asList( 1, 2);
   // private static final java.util.List<Integer> SYNC_TABLE_SELECTION = java.util.Arrays.asList(0);

    private static final boolean USE_INDEX_FRIENDLY_WHERE = true;

    //sqlserver表的条件
    private static final String WHERE_INDEX_FRIENDLY = "AddDate >= '2024-01-01' AND AddDate < '2025-11-01'";
    private static final String WHERE_FUNCTION_MODE = "LEFT(CONVERT(VARCHAR(8), AddDate, 112), 6) >= '202401' AND LEFT(CONVERT(VARCHAR(8), AddDate, 112), 6) <= '202510'";
    private static String sqlServerWhereFilter() { return USE_INDEX_FRIENDLY_WHERE ? WHERE_INDEX_FRIENDLY : WHERE_FUNCTION_MODE; }

    public static void main(String[] args) {
        System.out.println("开始同步任务...");
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
        System.out.println("使用排序字段: " + ORDER_BY_COLUMNS);
        System.out.println("SQL Server WHERE 过滤: " + sqlServerWhereFilter());
        System.out.println("同步表选择: " + SYNC_TABLE_SELECTION);

        if (total == 0) {
            System.out.println("无配置可同步，任务结束。");
            return;
        }

        int success = 0;
        int failed = 0;
        java.util.List<String> missingBase = new java.util.ArrayList<>();
        java.util.List<String> missingTy = new java.util.ArrayList<>();
        java.util.List<String> missingHy = new java.util.ArrayList<>();

        for (int i = 0; i < total; i++) {
            SyncConfig cfg = configs.get(i);
            String base = cfg.getTableName();

            SqlServerConnInfo ss = parseSqlServerConnection(cfg.getSqlServerConnection());
            SchemaTable stBase = extractSchemaAndTable(base);
            String schema = stBase.schema != null ? stBase.schema : (ss.schema != null ? ss.schema : "dbo");
            String basePure = stBase.table;

            java.util.List<String> presentTablesForTask = new java.util.ArrayList<>();

            try (Connection conn = openSqlServerConnection(ss)) {
                for (Integer sel : SYNC_TABLE_SELECTION) {
                    if (sel == null) continue;
                    String candidatePure;
                    String mark;
                    if (sel == 0) { candidatePure = basePure; mark = "BASE"; }
                    else if (sel == 1) { candidatePure = basePure + "ty"; mark = "TY"; }
                    else if (sel == 2) { candidatePure = basePure + "hy"; mark = "HY"; }
                    else { throw new IllegalArgumentException("非法选择值: " + sel + "，有效值为 0(原表),1(ty),2(hy)"); }

                    if (tableExists(conn, schema, candidatePure)) {
                        presentTablesForTask.add(stBase.schema != null ? stBase.schema + "." + candidatePure : candidatePure);
                    } else {
                        String idstr = String.format("%s.%s (Channel=%s, IndustryId=%d)", schema, candidatePure, cfg.getChannel(), cfg.getIndustryId());
                        if (sel == 0) missingBase.add(idstr);
                        else if (sel == 1) missingTy.add(idstr);
                        else missingHy.add(idstr);
                        System.out.printf("[%d/%d] 源%s表不存在，跳过: %s%n", i + 1, total, mark, idstr);
                    }
                }
            } catch (Exception ex) {
                System.err.printf("[%d/%d] 连接SQL Server失败，跳过该配置: %s%n", i + 1, total, ex.getMessage());
                failed++;
                continue;
            }

            if (presentTablesForTask.isEmpty()) {
                System.out.printf("[%d/%d] 该配置选择的表均不存在，跳过：Channel=%s, baseTable=%s%n", i + 1, total, cfg.getChannel(), base);
                // 不计失败，继续下一条配置
                continue;
            }

            try {
                for (String tName : presentTablesForTask) {
                    SyncConfig realCfg = new SyncConfig(cfg.getChannel(), cfg.getIndustryId(), cfg.getSqlServerConnection(), tName);
                    boolean ok = new TableSyncTask(
                            realCfg,
                            CLICKHOUSE_URL,
                            CLICKHOUSE_USER,
                            CLICKHOUSE_PASSWORD,
                            TARGET_TABLE,
                            PAGE_SIZE,
                            i + 1,
                            total,
                            THREADS,
                            ORDER_BY_COLUMNS,
                            sqlServerWhereFilter()
                    ).call();
                    if (!ok) {
                        throw new IllegalStateException("单表同步返回失败: " + tName);
                    }
                }

                long sqlSum = sumSqlServerCounts(cfg.getSqlServerConnection(), schema, presentTablesForTask, sqlServerWhereFilter());
                long ckSum = sumClickHouseCounts(TARGET_TABLE, cfg.getChannel(), presentTablesForTask);
                if (sqlSum != ckSum) {
                    System.err.printf("[%d/%d] 汇总不一致！SQL Server(存在表总和): %d 条, ClickHouse: %d 条。Channel=%s, tables=%s%n",
                            i + 1, total, sqlSum, ckSum, cfg.getChannel(), String.join(",", presentTablesForTask));
                    failed++;
                } else {
                    System.out.printf("[%d/%d] 汇总校验通过：SQL Server: %d 条, ClickHouse: %d 条 ✓  Channel=%s, tables=%s%n",
                            i + 1, total, sqlSum, ckSum, cfg.getChannel(), String.join(",", presentTablesForTask));
                    success++;
                }
            } catch (Exception ex) {
                failed++;
                System.err.printf("[%d/%d] 行配置任务失败: Channel=%s, baseTable=%s, 错误=%s%n",
                        i + 1, total, cfg.getChannel(), base, ex.getMessage());
            }
        }

        System.out.println("缺失 BASE 表列表：");
        if (missingBase.isEmpty()) { System.out.println(" - <无>"); } else { for (String s : missingBase) System.out.println(" - " + s); }
        System.out.println("缺失 TY 表列表：");
        if (missingTy.isEmpty()) { System.out.println(" - <无>"); } else { for (String s : missingTy) System.out.println(" - " + s); }
        System.out.println("缺失 HY 表列表：");
        if (missingHy.isEmpty()) { System.out.println(" - <无>"); } else { for (String s : missingHy) System.out.println(" - " + s); }

        System.out.printf("所有任务完成！成功: %d 个，失败: %d 个%n", success, failed);
    }

    // 检查表是否存在（schema + 纯表名）
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

    // 汇总 SQL Server 计数（仅统计存在表；应用统一 WHERE 过滤）
    private static long sumSqlServerCounts(String sqlServerConnStr, String schema, java.util.List<String> tableNames, String where) throws SQLException {
        SqlServerConnInfo ss = parseSqlServerConnection(sqlServerConnStr);
        try (Connection conn = openSqlServerConnection(ss)) {
            long sum = 0;
            for (String t : tableNames) {
                SchemaTable st = extractSchemaAndTable(t);
                String s = st.schema != null ? st.schema : schema;
                String qualified = "[" + s + "].[" + st.table + "]";
                // 修改：调用重命名后的方法，避免与重复定义产生冲突
                sum += querySqlServerCountWithFilter(conn, qualified, where);
            }
            return sum;
        }
    }

    // 汇总 ClickHouse 计数（Channel + tableName IN (存在表)）
    private static long sumClickHouseCounts(String targetTable, String channel, java.util.List<String> tableNames) throws SQLException {
        try (Connection ck = openClickHouseConnection()) {
            String placeholders = String.join(",", java.util.Collections.nCopies(tableNames.size(), "?"));
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

    // 修改：重命名后的辅助方法，同时打印汇总计数的 SQL
    private static long querySqlServerCountWithFilter(Connection conn, String qualifiedTable, String where) throws SQLException {
        String sql = "SELECT COUNT(*) FROM " + qualifiedTable + (where != null && !where.trim().isEmpty() ? " WHERE " + where : "");
        System.out.println("SQL Server 汇总计数SQL: " + sql);
        try (java.sql.Statement st = conn.createStatement();
             java.sql.ResultSet rs = st.executeQuery(sql)) {
            rs.next();
            return rs.getLong(1);
        }
    }

    // 帮助方法：以下与 TableSyncTask 内部一致，用于汇总统计
    private static SqlServerConnInfo parseSqlServerConnection(String connStr) {
        java.util.Map<String, String> kv = new java.util.HashMap<>();
        for (String part : connStr.split(";")) {
            part = part.trim();
            if (part.isEmpty()) continue;
            int idx = part.indexOf('=');
            if (idx <= 0) continue;
            kv.put(part.substring(0, idx).trim().toLowerCase(java.util.Locale.ENGLISH), part.substring(idx + 1).trim());
        }
        String ds = firstNonNull(kv.get("data source"), kv.get("server"));
        String host = null; int port = 1433;
        if (ds != null) {
            if (ds.contains(",")) { String[] hp = ds.split(",", 2); host = hp[0].trim(); try { port = Integer.parseInt(hp[1].trim()); } catch (Exception ignored) {} }
            else if (ds.contains("\\")) { host = ds.split("\\\\", 2)[0].trim(); } else { host = ds.trim(); }
        }
        String database = firstNonNull(kv.get("initial catalog"), kv.get("database"), kv.get("database name"));
        String user = firstNonNull(kv.get("user id"), kv.get("uid"), kv.get("user"));
        String password = firstNonNull(kv.get("password"), kv.get("pwd"));
        String schema = kv.getOrDefault("schema", "dbo");
        SqlServerConnInfo info = new SqlServerConnInfo(); info.host = host; info.port = port; info.database = database; info.user = user; info.password = password; info.schema = schema; return info;
    }

    private static String firstNonNull(String... vals) { for (String v : vals) { if (v != null && !v.isEmpty()) return v; } return null; }

    private static class SqlServerConnInfo { String host; int port = 1433; String database; String user; String password; String schema = "dbo"; }

    private static class SchemaTable { final String schema; final String table; SchemaTable(String s, String t) { this.schema = s; this.table = t; } }

    private static SchemaTable extractSchemaAndTable(String tableName) {
        String name = tableName.trim();
        if (name.contains(".")) { String[] parts = name.split("\\.", 2); return new SchemaTable(parts[0], parts[1]); }
        return new SchemaTable(null, name);
    }

    private static Connection openSqlServerConnection(SqlServerConnInfo info) throws SQLException {
        try { Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver"); } catch (Throwable ignored) {}
        String url = "jdbc:sqlserver://" + info.host + ":" + info.port + ";databaseName=" + info.database + ";encrypt=false;trustServerCertificate=true;loginTimeout=30";
        java.util.Properties props = new java.util.Properties(); props.setProperty("user", info.user); props.setProperty("password", info.password);
        return java.sql.DriverManager.getConnection(url, props);
    }

    private static Connection openClickHouseConnection() throws SQLException {
        java.util.Properties props = new java.util.Properties(); props.setProperty("user", CLICKHOUSE_USER); props.setProperty("password", CLICKHOUSE_PASSWORD);
        try { Class.forName("com.clickhouse.jdbc.ClickHouseDriver"); } catch (Throwable ignored) {}
        try { Class.forName("ru.yandex.clickhouse.ClickHouseDriver"); } catch (Throwable ignored) {}
        try { Class.forName("com.github.housepower.jdbc.ClickHouseDriver"); } catch (Throwable ignored) {}
        return java.sql.DriverManager.getConnection(CLICKHOUSE_URL, props);
    }

    private static long querySqlServerTotalCountWithFilter(Connection conn, String qualifiedTable, String where) throws SQLException {
        String sql = "SELECT COUNT(*) FROM " + qualifiedTable + (where != null && !where.trim().isEmpty() ? " WHERE " + where : "");
        try (java.sql.Statement st = conn.createStatement(); java.sql.ResultSet rs = st.executeQuery(sql)) { rs.next(); return rs.getLong(1); }
    }

    private static List<SyncConfig> loadConfigsFromMySQL() throws SQLException {
        String baseSql = "SELECT Channel, IndustryId, sqlServerConnection, tableName FROM " + MYSQL_CONFIG_TABLE;
        String whereClause = MYSQL_CONFIG_WHERE == null ? "" : MYSQL_CONFIG_WHERE.trim();
        if (!whereClause.isEmpty() && !whereClause.toLowerCase().startsWith("where ")) {
            // 支持仅写条件，自动补上 WHERE
            whereClause = "WHERE " + whereClause;
        }
        String sql = baseSql + (whereClause.isEmpty() ? "" : " " + whereClause);
        System.out.println("使用配置过滤条件: " + (whereClause.isEmpty() ? "<无>" : whereClause));

        try (Connection conn = DriverManager.getConnection(MYSQL_URL, MYSQL_USER, MYSQL_PASSWORD);
             Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery(sql)) {
            List<SyncConfig> list = new ArrayList<>();
            while (rs.next()) {
                list.add(SyncConfig.fromResultSet(rs));
            }
            return list;
        }
    }

    private static void tryLoadClassQuietly(String className) {
        try {
            Class.forName(className);
        } catch (Throwable ignored) {
        }
    }
}