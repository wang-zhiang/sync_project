package sibo.sync_detail_ck;

import java.math.BigDecimal;
import java.sql.*;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;

public class TableSyncTask implements java.util.concurrent.Callable<Boolean> {
    private final SyncConfig cfg;
    private final String clickHouseUrl;
    private final String clickHouseUser;
    private final String clickHousePassword;
    private final String targetTable; // e.g., test.ceshi_1110
    private final int pageSize;
    private final int taskIndex;
    private final int totalConfigs;
    private final int concurrency; // 单表分页并发数
    private final String orderByColumns; // 新增：排序字段（逗号分隔）
    private final String whereFilter;    // 新增：SQL Server 过滤条件（索引友好）

    public TableSyncTask(SyncConfig cfg,
                         String clickHouseUrl,
                         String clickHouseUser,
                         String clickHousePassword,
                         String targetTable,
                         int pageSize,
                         int taskIndex,
                         int totalConfigs,
                         int concurrency,
                         String orderByColumns,
                         String whereFilter) {
        this.cfg = cfg;
        this.clickHouseUrl = clickHouseUrl;
        this.clickHouseUser = clickHouseUser;
        this.clickHousePassword = clickHousePassword;
        this.targetTable = targetTable;
        this.pageSize = pageSize;
        this.taskIndex = taskIndex;
        this.totalConfigs = totalConfigs;
        this.concurrency = concurrency;
        this.orderByColumns = orderByColumns;
        this.whereFilter = whereFilter;
    }

    @Override
    public Boolean call() throws Exception {
        System.out.printf("[%d/%d] 开始同步: Channel=%s, IndustryId=%d, tableName=%s%n",
                taskIndex, totalConfigs, cfg.getChannel(), cfg.getIndustryId(), cfg.getTableName());

        SqlServerConnInfo ssInfo = parseSqlServerConnection(cfg.getSqlServerConnection());
        String qualifiedTable = qualifySqlServerTableName(cfg.getTableName(), ssInfo.schema);
        // 新增：解析并输出端点信息
        ClickHouseConnInfo ckInfo = parseClickHouseUrl(clickHouseUrl);
        SchemaTable stLog = extractSchemaAndTable(cfg.getTableName());
        String schemaLog = stLog.schema != null ? stLog.schema : (ssInfo.schema != null ? ssInfo.schema : "dbo");
        System.out.printf(
                "[%d/%d] 源SQLServer: %s:%d/%s.%s，表=%s；目标ClickHouse: %s:%d/%s，目标表=%s%n",
                taskIndex, totalConfigs,
                ssInfo.host, ssInfo.port, ssInfo.database, schemaLog, qualifiedTable,
                ckInfo.host, ckInfo.port, ckInfo.database, targetTable
        );
        System.out.printf("[%d/%d] 使用排序字段: %s；WHERE过滤: %s%n", taskIndex, totalConfigs, orderByColumns, (whereFilter == null || whereFilter.isEmpty()) ? "<无>" : whereFilter);

        try (Connection sqlServerConn = openSqlServerConnection(ssInfo)) {
            validateRequiredColumns(sqlServerConn, ssInfo, cfg.getTableName());
            System.out.printf("[%d/%d] 验证字段...通过%n", taskIndex, totalConfigs);
        }

        long totalRows;
        try (Connection sqlServerConn = openSqlServerConnection(ssInfo)) {
            totalRows = querySqlServerTotalCountWithFilter(sqlServerConn, qualifiedTable, whereFilter);
        }
        System.out.printf("[%d/%d] 总记录数(过滤后): %d%n", taskIndex, totalConfigs, totalRows);
        long pages = (totalRows + pageSize - 1) / pageSize;
        if (pages == 0) {
            // 空表直接校验
            try (Connection ck = openClickHouseConnection()) {
                long ckCount = queryClickHouseCount(ck, targetTable, cfg.getChannel(), cfg.getTableName());
                if (ckCount != 0) {
                    throw new IllegalStateException(String.format("[%d/%d] 数据不一致！SQL Server: 0 条, ClickHouse: %d 条。Channel=%s, tableName=%s",
                            taskIndex, totalConfigs, ckCount, cfg.getChannel(), cfg.getTableName()));
                }
            }
            System.out.printf("[%d/%d] SQL Server: 0 条, ClickHouse: 0 条 ✓%n", taskIndex, totalConfigs);
            return true;
        }

        // 单表内部并发分页
        ExecutorService pagePool = java.util.concurrent.Executors.newFixedThreadPool(concurrency);
        java.util.List<java.util.concurrent.Future<Integer>> futures = new java.util.ArrayList<>();

        for (int p = 1; p <= pages; p++) {
            long startRow = (long) (p - 1) * pageSize + 1;
            long endRow = Math.min((long) p * pageSize, totalRows);
            futures.add(pagePool.submit(new PageJob(p, (int) pages, startRow, endRow, ssInfo, qualifiedTable)));
        }

        int insertedSum = 0;
        try {
            for (java.util.concurrent.Future<Integer> f : futures) {
                insertedSum += f.get();
            }
        } finally {
            pagePool.shutdown();
        }

        System.out.printf("[%d/%d] 同步完成，验证数据量...%n", taskIndex, totalConfigs);
        try (Connection ck = openClickHouseConnection()) {
            long ckCount = queryClickHouseCount(ck, targetTable, cfg.getChannel(), cfg.getTableName());
            if (ckCount != totalRows) {
                String detail = String.format(java.util.Locale.CHINA,
                        "[%d/%d] 数据不一致！SQL Server: %d 条, ClickHouse: %d 条。Channel=%s, tableName=%s",
                        taskIndex, totalConfigs, totalRows, ckCount, cfg.getChannel(), cfg.getTableName());
                throw new IllegalStateException(detail);
            }
        }

        System.out.printf("[%d/%d] SQL Server: %d 条, ClickHouse: %d 条 ✓%n",
                taskIndex, totalConfigs, totalRows, totalRows);

        return true;
    }

    // 页任务：使用 ROW_NUMBER() 分页，独立连接读取并写入
    private class PageJob implements java.util.concurrent.Callable<Integer> {
        private final int pageNo;
        private final int totalPages;
        private final long startRow;
        private final long endRow;
        private final SqlServerConnInfo ssInfo;
        private final String qualifiedTable;

        PageJob(int pageNo, int totalPages, long startRow, long endRow,
                SqlServerConnInfo ssInfo, String qualifiedTable) {
            this.pageNo = pageNo;
            this.totalPages = totalPages;
            this.startRow = startRow;
            this.endRow = endRow;
            this.ssInfo = ssInfo;
            this.qualifiedTable = qualifiedTable;
        }

        @Override
        public Integer call() throws Exception {
            System.out.printf("[%d/%d] 开始分页同步: 第 %d/%d 页...%n", taskIndex, totalConfigs, pageNo, totalPages);

            // 每页独立连接，避免跨线程共享连接导致并发问题
            try (Connection sqlConn = openSqlServerConnection(ssInfo);
                 Connection ckConn = openClickHouseConnection()) {

                ckConn.setAutoCommit(false);
                String insertSql = "INSERT INTO " + targetTable +
                        " (Channel, IndustryId, tableName, Itemid, Itemname, Adddate, price, qty, zkprice, ImagesUrl) " +
                        " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

                String pageSql =
                        "SELECT Itemid, Itemname, Adddate, price, qty, zkprice, ImagesUrl " +
                        "FROM (" +
                        "  SELECT Itemid, Itemname, Adddate, price, qty, zkprice, ImagesUrl, " +
                        "         ROW_NUMBER() OVER (ORDER BY " + orderByColumns + ") AS rn " +
                        "  FROM " + qualifiedTable +
                        (whereFilter != null && !whereFilter.trim().isEmpty() ? " WHERE " + whereFilter : "") +
                        ") AS t " +
                        "WHERE t.rn BETWEEN ? AND ?";

                try (PreparedStatement ps = sqlConn.prepareStatement(pageSql);
                     PreparedStatement chInsert = ckConn.prepareStatement(insertSql)) {

                    ps.setLong(1, startRow);
                    ps.setLong(2, endRow);

                    int batchCount = 0;
                    try (ResultSet rs = ps.executeQuery()) {
                        while (rs.next()) {
                            SyncDataRecord record = SyncDataRecord.fromSqlServerRow(cfg, rs);
                            record.bindToPreparedStatement(chInsert);
                            chInsert.addBatch();
                            batchCount++;
                        }
                    }

                    if (batchCount > 0) {
                        chInsert.executeBatch();
                        ckConn.commit();
                    }

                    return batchCount;
                }
            }
        }
    }

    private void validateRequiredColumns(Connection conn, SqlServerConnInfo info, String tableName) throws SQLException {
        String[] requiredColumns = new String[]{
                "Id", "Itemid", "Itemname", "Adddate", "price", "qty", "zkprice", "ImagesUrl"
        };
        SchemaTable st = extractSchemaAndTable(tableName);
        String schema = st.schema != null ? st.schema : (info.schema != null ? info.schema : "dbo");
        String pureTable = st.table;

        String checkSql = "SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS " +
                "WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND COLUMN_NAME = ?";
        try (PreparedStatement ps = conn.prepareStatement(checkSql)) {
            // 校验固定必需字段
            for (String col : requiredColumns) {
                ps.setString(1, schema);
                ps.setString(2, pureTable);
                ps.setString(3, col);
                try (ResultSet rs = ps.executeQuery()) {
                    rs.next();
                    int exists = rs.getInt(1);
                    if (exists == 0) {
                        throw new SQLException("缺少必需字段：" + col + "，表：" + schema + "." + pureTable);
                    }
                }
            }
            // 新增：校验排序字段（逗号分隔）
            for (String sortCol : parseOrderColumns(orderByColumns)) {
                ps.setString(1, schema);
                ps.setString(2, pureTable);
                ps.setString(3, sortCol);
                try (ResultSet rs = ps.executeQuery()) {
                    rs.next();
                    int exists = rs.getInt(1);
                    if (exists == 0) {
                        throw new SQLException("缺少排序字段：" + sortCol + "，表：" + schema + "." + pureTable);
                    }
                }
            }
        }
    }

    // 解析排序字段（仅支持逗号分隔的纯列名）
    private java.util.List<String> parseOrderColumns(String orderBy) {
        java.util.List<String> cols = new java.util.ArrayList<>();
        if (orderBy == null || orderBy.trim().isEmpty()) {
            throw new IllegalArgumentException("排序字段不能为空");
        }
        for (String part : orderBy.split(",")) {
            String c = part.trim();
            if (c.isEmpty()) continue;
            // 简单防护：不支持表达式/函数
            if (c.contains("(") || c.contains(")") || c.contains(" ")) {
                throw new IllegalArgumentException("排序字段仅支持纯列名，逗号分隔；非法项：" + c);
            }
            cols.add(c);
        }
        return cols;
    }

    private long querySqlServerTotalCountWithFilter(Connection conn, String qualifiedTable, String where) throws SQLException {
        String sql = "SELECT COUNT(*) FROM " + qualifiedTable + (where != null && !where.trim().isEmpty() ? " WHERE " + where : "");
        // 输出用于统计总量的 SQL，便于核对
        System.out.printf("[%d/%d] SQL Server 计数SQL: %s%n", taskIndex, totalConfigs, sql);
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery(sql)) {
            rs.next();
            return rs.getLong(1);
        }
    }

    private long queryClickHouseCount(Connection ckConn, String targetTable, String channel, String tableName) throws SQLException {
        String sql = "SELECT COUNT(*) FROM " + targetTable + " WHERE Channel=? AND tableName=?";
        try (PreparedStatement ps = ckConn.prepareStatement(sql)) {
            ps.setString(1, channel);
            ps.setString(2, tableName);
            try (ResultSet rs = ps.executeQuery()) {
                rs.next();
                return rs.getLong(1);
            }
        }
    }

    private Connection openClickHouseConnection() throws SQLException {
        Properties props = new Properties();
        props.setProperty("user", clickHouseUser);
        props.setProperty("password", clickHousePassword);
        // 兼容不同 ClickHouse JDBC
        tryLoadClassQuietly("com.clickhouse.jdbc.ClickHouseDriver");
        tryLoadClassQuietly("ru.yandex.clickhouse.ClickHouseDriver");
        tryLoadClassQuietly("com.github.housepower.jdbc.ClickHouseDriver");
        return DriverManager.getConnection(clickHouseUrl, props);
    }

    private Connection openSqlServerConnection(SqlServerConnInfo info) throws SQLException {
        tryLoadClassQuietly("com.microsoft.sqlserver.jdbc.SQLServerDriver");
        String url = "jdbc:sqlserver://" + info.host + ":" + info.port +
                ";databaseName=" + info.database +
                ";encrypt=false;trustServerCertificate=true;loginTimeout=30";
        Properties props = new Properties();
        props.setProperty("user", info.user);
        props.setProperty("password", info.password);
        return DriverManager.getConnection(url, props);
    }

    private void tryLoadClassQuietly(String className) {
        try {
            Class.forName(className);
        } catch (Throwable ignored) {
        }
    }

    private String qualifySqlServerTableName(String tableName, String defaultSchema) {
        SchemaTable st = extractSchemaAndTable(tableName);
        String schema = st.schema != null ? st.schema : (defaultSchema != null ? defaultSchema : "dbo");
        return "[" + schema + "].[" + st.table + "]";
    }

    private SchemaTable extractSchemaAndTable(String tableName) {
        if (tableName == null) throw new IllegalArgumentException("tableName 不能为空");
        String name = tableName.trim();
        if (name.contains(".")) {
            String[] parts = name.split("\\.", 2);
            return new SchemaTable(parts[0], parts[1]);
        }
        return new SchemaTable(null, name);
    }

    private SqlServerConnInfo parseSqlServerConnection(String connStr) {
        if (connStr == null || connStr.isEmpty()) {
            throw new IllegalArgumentException("sqlServerConnection 为空");
        }
        Map<String, String> kv = new HashMap<>();
        for (String part : connStr.split(";")) {
            part = part.trim();
            if (part.isEmpty()) continue;
            int idx = part.indexOf('=');
            if (idx <= 0) continue;
            String key = part.substring(0, idx).trim();
            String value = part.substring(idx + 1).trim();
            kv.put(key.toLowerCase(Locale.ENGLISH), value);
        }

        String host = null, database = null, user = null, password = null, schema = null;
        int port = 1433;

        // Data Source or Server: "IP,Port" or "server\instance"
        String ds = firstNonNull(kv.get("data source"), kv.get("server"));
        if (ds != null) {
            if (ds.contains(",")) {
                String[] hp = ds.split(",", 2);
                host = hp[0].trim();
                try {
                    port = Integer.parseInt(hp[1].trim());
                } catch (NumberFormatException ignored) {
                    port = 1433;
                }
            } else if (ds.contains("\\")) {
                // server\instance -> use server, default port
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

    private String firstNonNull(String... vals) {
        for (String v : vals) {
            if (v != null && !v.isEmpty()) return v;
        }
        return null;
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

        SchemaTable(String schema, String table) {
            this.schema = schema;
            this.table = table;
        }
    }

    // 解析 ClickHouse JDBC URL，支持 jdbc:clickhouse://host:port/db[?params]
    private ClickHouseConnInfo parseClickHouseUrl(String url) {
        ClickHouseConnInfo info = new ClickHouseConnInfo();
        if (url == null || url.isEmpty()) return info;

        String s = url.trim();
        if (s.startsWith("jdbc:")) s = s.substring(5);           // 去掉 "jdbc:"
        int schemeIdx = s.indexOf("://");
        String rest = schemeIdx >= 0 ? s.substring(schemeIdx + 3) : s;

        int slash = rest.indexOf('/');
        String hostPort = slash >= 0 ? rest.substring(0, slash) : rest;
        String dbAndParams = slash >= 0 ? rest.substring(slash + 1) : "";

        int colon = hostPort.indexOf(':');
        if (colon >= 0) {
            info.host = hostPort.substring(0, colon);
            try {
                info.port = Integer.parseInt(hostPort.substring(colon + 1));
            } catch (NumberFormatException ignored) {
                info.port = 8123;
            }
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

    private static class ClickHouseConnInfo {
        String host = "unknown";
        int port = 8123;
        String database = "default";
    }
}