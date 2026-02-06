package sibo.ck_utll;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;


//处理source4的imageurl

public class ClickHouseSyncJob2 {

    // =========================================================================
    //                          配置区域 (请在此处修改)
    // =========================================================================

    // ClickHouse 连接信息
    private static final String DB_HOST = "hadoop110";
    private static final String DB_PORT = "8123";
    private static final String DB_NAME = "dwd";
    private static final String DB_USER = "default";
    private static final String DB_PASSWORD = "smartpath";

    // 业务变量配置
    // 左表 (dwd.thirddata_summary) 的筛选条件
    private static final String VAR_PT_PLATFORM = "S4";
    private static final String VAR_PT_CHANNEL_LEFT = "jd";   // 左表渠道

    // 右表 (dwd.source4) 的筛选条件
    private static final String VAR_PT_CHANNEL_RIGHT = "jd";  // 右表渠道 (dwd.source4)

    // 超时设置 (毫秒) - 当前设置为 2小时
    private static final int SOCKET_TIMEOUT = 2 * 60 * 60 * 1000;

    // =========================================================================

    public static void main(String[] args) {
        Connection conn = null;
        String jdbcUrl = String.format("jdbc:clickhouse://%s:%s/%s", DB_HOST, DB_PORT, DB_NAME);

        try {
            // 1. 初始化连接配置
            Properties properties = new Properties();
            properties.setProperty("user", DB_USER);
            properties.setProperty("password", DB_PASSWORD);
            properties.setProperty("socket_timeout", String.valueOf(SOCKET_TIMEOUT));
            properties.setProperty("queryTimeout", String.valueOf(SOCKET_TIMEOUT / 1000));

            System.out.println("==================================================");
            System.out.println("正在连接 ClickHouse: " + jdbcUrl);
            conn = DriverManager.getConnection(jdbcUrl, properties);
            System.out.println("连接成功！当前配置：");
            System.out.println("   Left Platform : " + VAR_PT_PLATFORM);
            System.out.println("   Left Channel  : " + VAR_PT_CHANNEL_LEFT);
            System.out.println("   Right Channel : " + VAR_PT_CHANNEL_RIGHT);
            System.out.println("==================================================");

            // 2. 获取任务列表 (遍历所有月份)
            List<String> monthList = getMonthList(conn);
            int totalMonths = monthList.size();

            System.out.println("检测到符合条件的月份总数: " + totalMonths);
            System.out.println("待处理月份: " + monthList);
            System.out.println("--------------------------------------------------");

            if (totalMonths == 0) {
                System.out.println("没有需要处理的数据，任务结束。");
                return;
            }

            // 3. 循环执行 Insert 任务
            for (int i = 0; i < totalMonths; i++) {
                String currentYm = monthList.get(i);
                long startTime = System.currentTimeMillis();

                System.out.println(String.format(">>> 进度 [%d/%d] | 正在执行月份: %s", (i + 1), totalMonths, currentYm));
                System.out.println("    状态: SQL 执行中 (包含子查询去重)...");

                try {
                    // 执行核心 SQL
                    int rows = executeInsertSql(conn, currentYm);

                    long endTime = System.currentTimeMillis();
                    double durationSeconds = (endTime - startTime) / 1000.0;

                    System.out.println(String.format("    结果: ✅ 成功 (影响行数: %d)", rows));
                    System.out.println(String.format("    耗时: %.2f 秒", durationSeconds));

                } catch (SQLException e) {
                    System.err.println(String.format("    结果: ❌ 失败! 月份 %s 执行出错。", currentYm));
                    System.err.println("    错误信息: " + e.getMessage());
                }
                System.out.println("--------------------------------------------------");
            }

            System.out.println("所有任务执行完毕！");

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    /**
     * 获取 distinct pt_ym 列表
     */
    private static List<String> getMonthList(Connection conn) throws SQLException {
        List<String> months = new ArrayList<>();
        String querySql = "SELECT DISTINCT pt_ym FROM dwd.thirddata_summary " +
                "WHERE pt_platform = ? AND pt_channel = ? " +
                "ORDER BY pt_ym";

        try (PreparedStatement ps = conn.prepareStatement(querySql)) {
            ps.setString(1, VAR_PT_PLATFORM);
            ps.setString(2, VAR_PT_CHANNEL_LEFT);

            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    months.add(rs.getString("pt_ym"));
                }
            }
        }
        return months;
    }

    /**
     * 拼接并执行大 SQL (内嵌复杂子查询)
     */
    private static int executeInsertSql(Connection conn, String ym) throws SQLException {
        StringBuilder sqlBuilder = new StringBuilder();

        sqlBuilder.append("INSERT INTO dwd.thirddata_summary_new ");

        // --- SELECT 部分 ---
        sqlBuilder.append("SELECT ");
        sqlBuilder.append("    Source, Server, DateType, ShopName, ItemName, ItemId, ");
        sqlBuilder.append("    b.images AS Images, "); // 这里取的是 b 表 (子查询) 的 images
        sqlBuilder.append("    Industry, Category, Brand, Value000, Qty, Price, PagePrice, ");
        sqlBuilder.append("    ShopType, RootCategoryName, CategoryName, SubCategoryName, ");
        sqlBuilder.append("    ThirdBrand, ItemIdOld, TType, CategoryId, BrandId, ");
        sqlBuilder.append("    Source5QueryTime, YM, pt_ym, pt_channel, pt_platform ");

        // --- FROM 左表 (a) ---
        sqlBuilder.append("FROM ");
        sqlBuilder.append("( ");
        sqlBuilder.append("    SELECT * FROM dwd.thirddata_summary ");
        sqlBuilder.append("    WHERE pt_platform = '").append(VAR_PT_PLATFORM).append("' ");
        sqlBuilder.append("      AND pt_channel = '").append(VAR_PT_CHANNEL_LEFT).append("' ");
        sqlBuilder.append("      AND pt_ym = '").append(ym).append("' ");
        sqlBuilder.append(") a ");

        // --- GLOBAL LEFT JOIN 右表子查询 (b) ---
        sqlBuilder.append("GLOBAL LEFT JOIN ");
        sqlBuilder.append("( ");
        sqlBuilder.append("    SELECT pt_ym, goods_id, images ");
        sqlBuilder.append("    FROM dwd.source4 ");
        // 动态变量: VAR_PT_CHANNEL_RIGHT
        sqlBuilder.append("    WHERE pt_channel = '").append(VAR_PT_CHANNEL_RIGHT).append("' ");
        sqlBuilder.append("      AND goods_id != '' ");
        sqlBuilder.append("      AND images != '' ");
        // 关键优化：右表也加上当前月份限制，防止全表扫描
        sqlBuilder.append("      AND pt_ym = '").append(ym).append("' ");
        sqlBuilder.append("    ORDER BY pt_ym DESC ");
        // 核心逻辑：取最后一条去重
        sqlBuilder.append("    LIMIT 1 BY pt_ym, goods_id ");
        sqlBuilder.append(") b ");

        // --- ON 条件 ---
        // 注意：这里 b 表的 ItemId 叫 goods_id
        sqlBuilder.append("ON a.pt_ym = b.pt_ym AND a.ItemId = b.goods_id");

        String finalSql = sqlBuilder.toString();

        // 调试打印 SQL (建议保留，方便排查问题)
        // System.out.println("DEBUG SQL: " + finalSql);

        try (Statement stmt = conn.createStatement()) {
            return stmt.executeUpdate(finalSql);
        }
    }
}