package sqlserver_util;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class sqlserver_mysql {

    public static void main(String[] args) {
        // SQL Server配置
        String sqlServerUrl = "jdbc:sqlserver://192.168.4.219;DatabaseName=datasystem";
        String sqlServerUser = "sa";
        String sqlServerPassword = "smartpthdata";

        // MySQL配置
        String mysqlUrl = "jdbc:mysql://192.168.3.138:3306/avene?useSSL=false&serverTimezone=UTC";
        String mysqlUser = "root";
        String mysqlPassword = "smartpthdata";

        // 自定义SELECT查询（可以替换为您需要的任何查询）
        String selectSql = "SELECT    Y,M,YM,YQ,Val,Vol,Value000,Volume000,skuid,IndustryId,IndustryName,IndustrySubId,IndustrySubName,SegmentId,Segment,BrandId,brandEnCn,brandEN,Brand,Manufacturer,VName,SeriesId,SKU,Segment3,Category2,Channel0,Channel1,Channel2    from  amplify_avene_massskincare_ES ";

        // 目标表名
        String targetTable = "amplify_avene_massskincare_ES_new";

        // 并发与批次参数（并发与批次参数（用于分页、线程池、批量写入与查询超时）
        int pageSize = 100_000;        // 每页拉取的行数；影响分页数量与单页处理时长
        int threads = 4;               // 并发线程数；同时处理的页任务数，受源库/目标库负载限制
        int batchSize = 5_000;         // MySQL批量提交阈值；达到该行数后执行executeBatch并commit
        int fetchSize = 10_000;        // JDBC提取批大小提示；流式读取时每次从服务器抓取的行数
        int queryTimeoutSeconds = 0;   // 查询超时（秒）；0表示不设超时
        // 下划线是Java的数字字面量分隔符，用于增强可读性，100_000等价于100000
        try {
            // 执行分页并发同步
            syncData(sqlServerUrl, sqlServerUser, sqlServerPassword,
                     mysqlUrl, mysqlUser, mysqlPassword,
                     selectSql, targetTable,
                     pageSize, threads, batchSize, fetchSize, queryTimeoutSeconds);

            System.out.println("数据同步完成！");
        } catch (Exception e) {
            System.err.println("数据同步失败: " + e.getMessage());
            e.printStackTrace();
        }
    }

    // 改造后的核心同步方法：分页 + 多线程 + 批次 + 自动建表
    public static void syncData(String sourceUrl, String sourceUser, String sourcePwd,
                                String targetUrl, String targetUser, String targetPwd,
                                String selectSql, String targetTable,
                                int pageSize, int threads, int batchSize, int fetchSize, int queryTimeoutSeconds) throws Exception {
        // 1) 用空结果获取列元数据（列名、类型、精度/标度/显示宽度）
        List<String> columnNames = new ArrayList<>();
        List<Integer> jdbcTypes = new ArrayList<>();
        List<Integer> precisions = new ArrayList<>();
        List<Integer> scales = new ArrayList<>();
        List<Integer> displaySizes = new ArrayList<>();

        try (Connection src = DriverManager.getConnection(sourceUrl, sourceUser, sourcePwd);
             Statement st = src.createStatement()) {

            String metaSql = "SELECT * FROM (" + selectSql + ") base WHERE 1=0";
            try (ResultSet rs = st.executeQuery(metaSql)) {
                ResultSetMetaData md = rs.getMetaData();
                int count = md.getColumnCount();
                for (int i = 1; i <= count; i++) {
                    columnNames.add(md.getColumnLabel(i));
                    jdbcTypes.add(md.getColumnType(i));
                    precisions.add(md.getPrecision(i));
                    scales.add(md.getScale(i));
                    displaySizes.add(md.getColumnDisplaySize(i));
                }
            }
        }

        // 2) 在 MySQL 上确保目标表存在（无 Id 字段则新增自增主键）
        try (Connection tgt = DriverManager.getConnection(targetUrl, targetUser, targetPwd)) {
            ensureTargetTable(tgt, targetTable, columnNames, jdbcTypes, precisions, scales, displaySizes);
        }

        // 3) 统计总行数并计算分页
        long totalRows = countRows(sourceUrl, sourceUser, sourcePwd, selectSql, queryTimeoutSeconds);
        if (totalRows <= 0) {
            System.out.println("源查询结果为空，结束。");
            return;
        }
        long pages = (totalRows + pageSize - 1) / pageSize;
        System.out.println("总行数: " + totalRows + "，页数: " + pages + "，并发线程: " + threads);

        // 4) 并发分页复制
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        List<Future<Long>> futures = new ArrayList<>();
        long startTime = System.currentTimeMillis();

        for (int i = 0; i < pages; i++) {
            final int pageIndex = i;
            final long start = (long) pageIndex * pageSize + 1;
            final long end = Math.min(start + pageSize - 1, totalRows);

            futures.add(pool.submit(() ->
                copyPage(sourceUrl, sourceUser, sourcePwd,
                         targetUrl, targetUser, targetPwd,
                         selectSql, targetTable,
                         columnNames, batchSize, fetchSize, queryTimeoutSeconds,
                         start, end, pageIndex)
            ));
        }

        pool.shutdown();

        long totalCopied = 0;
        for (int i = 0; i < futures.size(); i++) {
            try {
                totalCopied += futures.get(i).get();
            } catch (ExecutionException ee) {
                System.err.println("分页 " + i + " 失败: " + ee.getCause().getMessage());
                ee.getCause().printStackTrace();
            }
        }

        long elapsed = System.currentTimeMillis() - startTime;
        System.out.println("完成复制: " + totalCopied + " 行，耗时 " + (elapsed / 1000.0) + " 秒");
    }

    // 分页查询封装：对自定义查询加 RN，同时 BETWEEN ? AND ? 分页
    private static String buildPagedSelect(String baseSql) {
        return "SELECT * FROM (" +
               " SELECT base.*, ROW_NUMBER() OVER (ORDER BY (SELECT 1)) AS rn FROM (" + baseSql + ") base" +
               ") src WHERE rn BETWEEN ? AND ?";
    }

    // 统计总行数：对自定义查询外包一层
    private static long countRows(String sourceUrl, String sourceUser, String sourcePwd,
                                  String baseSql, int queryTimeoutSeconds) throws SQLException {
        try (Connection conn = DriverManager.getConnection(sourceUrl, sourceUser, sourcePwd);
             PreparedStatement ps = conn.prepareStatement("SELECT COUNT(*) FROM (" + baseSql + ") src")) {
            if (queryTimeoutSeconds > 0) ps.setQueryTimeout(queryTimeoutSeconds);
            try (ResultSet rs = ps.executeQuery()) {
                rs.next();
                return rs.getLong(1);
            }
        }
    }

    // 页复制：独立连接、批次提交
    private static long copyPage(String sourceUrl, String sourceUser, String sourcePwd,
                                 String targetUrl, String targetUser, String targetPwd,
                                 String baseSql, String targetTable,
                                 List<String> columnNames, int batchSize, int fetchSize, int queryTimeoutSeconds,
                                 long startRow, long endRow, int pageIndex) throws Exception {
        String selectSql = buildPagedSelect(baseSql);
        String insertSql = buildInsertSql(targetTable, columnNames);

        long copied = 0;
        int retries = 1;

        for (int attempt = 0; attempt <= retries; attempt++) {
            try (Connection src = DriverManager.getConnection(sourceUrl, sourceUser, sourcePwd);
                 Connection tgt = DriverManager.getConnection(targetUrl, targetUser, targetPwd);
                 PreparedStatement sps = src.prepareStatement(selectSql,
                         ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
                 PreparedStatement ips = tgt.prepareStatement(insertSql)) {

                if (queryTimeoutSeconds > 0) {
                    sps.setQueryTimeout(queryTimeoutSeconds);
                    ips.setQueryTimeout(queryTimeoutSeconds);
                }
                sps.setFetchSize(fetchSize);
                tgt.setAutoCommit(false);

                sps.setLong(1, startRow);
                sps.setLong(2, endRow);

                try (ResultSet rs = sps.executeQuery()) {
                    int colCount = columnNames.size();
                    int batchCount = 0;

                    while (rs.next()) {
                        for (int i = 1; i <= colCount; i++) {
                            ips.setObject(i, rs.getObject(i));
                        }
                        ips.addBatch();
                        batchCount++;
                        copied++;

                        if (batchCount >= batchSize) {
                            ips.executeBatch();
                            tgt.commit();
                            batchCount = 0;
                        }
                    }

                    if (batchCount > 0) {
                        ips.executeBatch();
                        tgt.commit();
                    }
                }

                System.out.println("分页 " + pageIndex + " 复制完成，行数: " + copied +
                        "，区间: [" + startRow + ", " + endRow + "]");
                return copied;
            } catch (SQLException e) {
                if (attempt < retries) {
                    long backoffMs = 1000L * (attempt + 1);
                    System.err.println("分页 " + pageIndex + " 失败重试: " + e.getMessage() +
                            "，等待 " + backoffMs + "ms 后重试");
                    Thread.sleep(backoffMs);
                } else {
                    throw e;
                }
            }
        }
        return copied;
    }

    // 若目标表不存在则按查询列自动建表；无 Id 列则新增自增主键
    private static void ensureTargetTable(Connection tgtConn, String targetTable,
                                          List<String> columnNames, List<Integer> jdbcTypes,
                                          List<Integer> precisions, List<Integer> scales,
                                          List<Integer> displaySizes) throws SQLException {
        boolean exists;
        try (PreparedStatement ps = tgtConn.prepareStatement(
                "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = DATABASE() AND table_name = ?")) {
            ps.setString(1, targetTable);
            try (ResultSet rs = ps.executeQuery()) {
                rs.next();
                exists = rs.getInt(1) > 0;
            }
        }
        if (exists) {
            System.out.println("目标表已存在: " + targetTable + "（不修改结构）");
            return;
        }

        boolean hasId = columnNames.stream().anyMatch(n -> n.equalsIgnoreCase("id"));
        String ddl = buildCreateTableSql(targetTable, columnNames, jdbcTypes, precisions, scales, displaySizes, !hasId);

        try (Statement st = tgtConn.createStatement()) {
            st.execute(ddl);
            System.out.println("已创建目标表: " + targetTable);
        }
    }

    // 生成 MySQL 建表 DDL（和查询字段一致；可选追加 Id 自增主键）
    private static String buildCreateTableSql(String tableName,
                                              List<String> columnNames, List<Integer> jdbcTypes,
                                              List<Integer> precisions, List<Integer> scales,
                                              List<Integer> displaySizes, boolean addAutoId) {
        StringBuilder sb = new StringBuilder();
        sb.append("CREATE TABLE IF NOT EXISTS ").append(quoteMySql(tableName)).append(" (");

        if (addAutoId) {
            sb.append(quoteMySql("Id")).append(" INT NOT NULL AUTO_INCREMENT, ");
        }

        for (int i = 0; i < columnNames.size(); i++) {
            String col = columnNames.get(i);
            String type = mapToMySqlType(jdbcTypes.get(i),
                    safeVal(precisions.get(i), 0),
                    safeVal(scales.get(i), 0),
                    safeVal(displaySizes.get(i), 0));
            sb.append(quoteMySql(col)).append(" ").append(type).append(" NULL");
            if (i < columnNames.size() - 1) sb.append(", ");
        }

        if (addAutoId) {
            sb.append(", PRIMARY KEY (").append(quoteMySql("Id")).append(")");
        }

        sb.append(")");
        return sb.toString();
    }

    private static int safeVal(Integer v, int def) { return v != null ? v : def; }

    private static String mapToMySqlType(int jdbcType, int precision, int scale, int displaySize) {
        switch (jdbcType) {
            case Types.VARCHAR:
            case Types.NVARCHAR:
                int vlen = precision > 0 ? precision : (displaySize > 0 ? Math.min(displaySize, 65535) : 255);
                vlen = Math.min(Math.max(vlen, 1), 65535);
                // 超过 65535 的用 TEXT
                return vlen > 65535 ? "TEXT" : "VARCHAR(" + vlen + ")";
            case Types.CHAR:
                int clen = precision > 0 ? precision : (displaySize > 0 ? displaySize : 1);
                clen = Math.min(Math.max(clen, 1), 255);
                return "CHAR(" + clen + ")";
            case Types.LONGVARCHAR:
            case Types.CLOB:
            case Types.NCLOB:
                return "TEXT";
            case Types.BLOB:
            case Types.LONGVARBINARY:
                return "LONGBLOB";
            case Types.VARBINARY:
            case Types.BINARY:
                int blen = precision > 0 ? precision : (displaySize > 0 ? displaySize : 255);
                blen = Math.min(Math.max(blen, 1), 65535);
                return blen > 65535 ? "LONGBLOB" : "VARBINARY(" + blen + ")";
            case Types.TINYINT:
                return "TINYINT";
            case Types.SMALLINT:
                return "SMALLINT";
            case Types.INTEGER:
                return "INT";
            case Types.BIGINT:
                return "BIGINT";
            case Types.NUMERIC:
            case Types.DECIMAL:
                int p = precision > 0 ? precision : 19;
                int s = scale > 0 ? scale : 4;
                return "DECIMAL(" + p + "," + s + ")";
            case Types.FLOAT:
            case Types.REAL:
                return "FLOAT";
            case Types.DOUBLE:
                return "DOUBLE";
            case Types.BIT:
            case Types.BOOLEAN:
                return "BIT";
            case Types.DATE:
                return "DATE";
            case Types.TIME:
                return "TIME";
            case Types.TIMESTAMP:
                return "DATETIME";
            default:
                // 兜底
                return "TEXT";
        }
    }

    private static String quoteMySql(String name) {
        return "`" + name.replace("`", "``") + "`";
    }

    // 更新后的插入 SQL：列名与查询一致，使用反引号保护
    private static String buildInsertSql(String tableName, List<String> columns) {
        StringBuilder sql = new StringBuilder("INSERT INTO ");
        sql.append(quoteMySql(tableName)).append(" (");

        for (int i = 0; i < columns.size(); i++) {
            if (i > 0) sql.append(", ");
            sql.append(quoteMySql(columns.get(i)));
        }

        sql.append(") VALUES (");
        for (int i = 0; i < columns.size(); i++) {
            if (i > 0) sql.append(", ");
            sql.append("?");
        }
        sql.append(")");
        return sql.toString();
    }
}