package kzb.source4_ldl_category;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class source4Ldljob2 {

    // 配置信息
    private static final String CK_URL = "jdbc:clickhouse://192.168.5.111:8123/default"
            + "?socket_timeout=3600000"       // 关键：客户端等待数据的超时时间 (1小时)
            + "&connection_timeout=60000"     // 连接建立的超时时间 (60秒)
            + "&max_execution_time=3600";     // 关键：服务端允许查询执行的最大时间 (3600秒)
    private static final String CK_USER = "default";
    private static final String CK_PASSWORD = "smartpath";

    // 定义渠道列表（按你需要的顺序）
    private static final List<String> CHANNELS = Arrays.asList(
            "'淘宝','天猫'",
            "'京东'",
            "'抖音'"
    );

    public static void main(String[] args) {
        try {
            Class.forName("ru.yandex.clickhouse.ClickHouseDriver");
            System.out.println(">>> 任务启动，连接地址: " + CK_URL);

            // ==========================================
            // 核心逻辑：最外层按【渠道】循环
            // ==========================================
            for (String channel : CHANNELS) {
                System.out.println("\n################################################");
                System.out.println(">>> 进入渠道处理循环: " + channel);
                System.out.println("################################################");

                try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {

                    // 1. 执行类目需求 (Category) - 对应之前的需求二
                    runCategoryLogic(stmt, channel);

                    // 2. 执行品牌需求 (Brand) - 对应之前的需求三
                    runBrandLogic(stmt, channel);

                    // 3. 执行店铺需求 (Shop) - 对应之前的需求一
                    runShopLogic(stmt, channel);

                    System.out.println("\n>>> 当前渠道 [" + channel + "] 所有 Update 指令已发送。");
                }

                // 4. 关键点：在这个渠道的所有业务跑完后，卡住，检查 Mutation
                // 只有等 mutation 清空了，for 循环才会进入下一次，处理下一个渠道
                checkAndBlockUntilDone();
            }

            System.out.println("\n>>> 所有渠道处理完毕，程序退出。");

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // ----------------------------------------------------------------
    // 业务逻辑模块 1：类目 (Category)
    // ----------------------------------------------------------------
    private static void runCategoryLogic(Statement stmt, String channel) throws SQLException {
        System.out.println("  [Task 1/3] 执行类目 (Category) 逻辑...");

        stmt.execute("DROP TABLE IF EXISTS ods.category_match_0911");

        String createSql = String.format(
                "CREATE TABLE ods.category_match_0911 ENGINE = MergeTree ORDER BY Itemid AS " +
                        "SELECT t1.Itemid, t1.SP_Category_I, t1.SP_Category_II, t1.SP_Category_III " +
                        "FROM (SELECT Itemid, SP_Category_I, SP_Category_II, SP_Category_III FROM tmp.category WHERE Channel IN (%s)) t1 " +
                        "GLOBAL JOIN ( " +
                        "    SELECT Itemid FROM tmp.category WHERE Channel IN (%s) " +
                        "    GROUP BY Itemid " +
                        "    HAVING COUNT(CASE WHEN SP_Category_I = '' THEN 1 END) > 0 " +
                        "    AND COUNT(CASE WHEN SP_Category_I != '' THEN 1 END) > 0 " +
                        ") t2 ON t1.Itemid = t2.Itemid " +
                        "WHERE t1.SP_Category_I != '' AND t1.SP_Category_I != 'NULL-下架' AND t1.SP_Category_I != 'NULL-删' AND Itemid != '' " +
                        "ORDER BY t1.SP_Category_I DESC LIMIT 1 BY Itemid",
                channel, channel);
        stmt.execute(createSql);

        stmt.execute("DROP TABLE IF EXISTS ods.category_match_0911_dict");
        stmt.execute("CREATE TABLE ods.category_match_0911_dict ENGINE = Join(ANY, LEFT, Itemid) AS SELECT * FROM ods.category_match_0911");

        String updateSql = String.format(
                "ALTER TABLE dwd.source4_ldl " +
                        "UPDATE SP_Category_I = joinGet('ods.category_match_0911_dict','SP_Category_I',Itemid), " +
                        "       SP_Category_II = joinGet('ods.category_match_0911_dict','SP_Category_II',Itemid), " +
                        "       SP_Category_III = joinGet('ods.category_match_0911_dict','SP_Category_III',Itemid) " +
                        "WHERE SP_Category_I = '' AND Channel IN (%s)",
                channel);
        stmt.execute(updateSql);
        System.out.println("    -> Category Update 已提交");
    }

    // ----------------------------------------------------------------
    // 业务逻辑模块 2：品牌 (Brand)
    // ----------------------------------------------------------------
    private static void runBrandLogic(Statement stmt, String channel) throws SQLException {
        System.out.println("  [Task 2/3] 执行品牌 (Brand) 逻辑...");

        stmt.execute("DROP TABLE IF EXISTS ods.brand_match_0911");

        String createSql = String.format(
                "CREATE TABLE ods.brand_match_0911 ENGINE = MergeTree ORDER BY Itemid AS " +
                        "SELECT t1.Itemid, t1.SP_Brand_CN, t1.SP_Brand_CNEN, t1.SP_Brand_EN " +
                        "FROM (SELECT Itemid, SP_Brand_CN, SP_Brand_CNEN, SP_Brand_EN FROM tmp.brand WHERE Channel IN (%s)) t1 " +
                        "GLOBAL JOIN ( " +
                        "    SELECT Itemid FROM tmp.brand WHERE Channel IN (%s) " +
                        "    GROUP BY Itemid " +
                        "    HAVING COUNT(CASE WHEN SP_Brand_CN = 'Other Brands' OR SP_Brand_CN = '' THEN 1 END) > 0 " +
                        "    AND COUNT(CASE WHEN SP_Brand_CN != 'Other Brands' AND SP_Brand_CN != '' THEN 1 END) > 0 " +
                        ") t2 ON t1.Itemid = t2.Itemid " +
                        "WHERE t1.SP_Brand_CN != 'Other Brands' AND t1.SP_Brand_CN != '' AND Itemid != '' " +
                        "ORDER BY t1.SP_Brand_CN DESC LIMIT 1 BY Itemid",
                channel, channel);
        stmt.execute(createSql);

        stmt.execute("DROP TABLE IF EXISTS ods.brand_match_0911_dict");
        stmt.execute("CREATE TABLE ods.brand_match_0911_dict ENGINE = Join(ANY, LEFT, Itemid) AS SELECT * FROM ods.brand_match_0911");

        String updateSql = String.format(
                "ALTER TABLE dwd.source4_ldl " +
                        "UPDATE SP_Brand_CNEN = joinGet('ods.brand_match_0911_dict','SP_Brand_CNEN',Itemid), " +
                        "       SP_Brand_CN = joinGet('ods.brand_match_0911_dict','SP_Brand_CN',Itemid), " +
                        "       SP_Brand_EN = joinGet('ods.brand_match_0911_dict','SP_Brand_EN',Itemid) " +
                        "WHERE (SP_Brand_CN = 'Other Brands' OR SP_Brand_CN = '') AND Channel IN (%s)",
                channel);
        stmt.execute(updateSql);
        System.out.println("    -> Brand Update 已提交");
    }

    // ----------------------------------------------------------------
    // 业务逻辑模块 3：店铺 (Shop)
    // ----------------------------------------------------------------
    private static void runShopLogic(Statement stmt, String channel) throws SQLException {
        System.out.println("  [Task 3/3] 执行店铺 (Shop) 逻辑...");

        stmt.execute("DROP TABLE IF EXISTS ods.shop_match_0911");

        String createSql = String.format(
                "CREATE TABLE ods.shop_match_0911 ENGINE = MergeTree ORDER BY Itemid AS " +
                        "SELECT t1.Itemid, t1.Shop " +
                        "FROM (SELECT Itemid, Shop FROM tmp.shop WHERE Channel IN (%s)) t1 " +
                        "GLOBAL JOIN ( " +
                        "    SELECT Itemid FROM tmp.shop WHERE Channel IN (%s) " +
                        "    GROUP BY Itemid " +
                        "    HAVING COUNT(CASE WHEN Shop = '' THEN 1 END) > 0 " +
                        "    AND COUNT(CASE WHEN Shop != '' THEN 1 END) > 0 " +
                        ") t2 ON t1.Itemid = t2.Itemid " +
                        "WHERE t1.Shop != '' AND Itemid != '' " +
                        "ORDER BY Shop DESC LIMIT 1 BY Itemid",
                channel, channel);
        stmt.execute(createSql);

        stmt.execute("DROP TABLE IF EXISTS ods.shop_match_0911_dict");
        stmt.execute("CREATE TABLE ods.shop_match_0911_dict ENGINE = Join(ANY, LEFT, Itemid) AS SELECT * FROM ods.shop_match_0911");

        String updateSql = String.format(
                "ALTER TABLE dwd.source4_ldl " +
                        "UPDATE Shop = joinGet('ods.shop_match_0911_dict','Shop',Itemid) " +
                        "WHERE Channel IN (%s) AND Itemid IN (SELECT Itemid FROM ods.shop_match_0911_dict) AND Shop = ''",
                channel);
        stmt.execute(updateSql);
        System.out.println("    -> Shop Update 已提交");
    }

    // ----------------------------------------------------------------
    // 检查 Mutations (阻塞方法)
    // ----------------------------------------------------------------
    private static void checkAndBlockUntilDone() throws SQLException, InterruptedException {
        System.out.print(">>> 正在检查后台 Mutations 状态");

        while (true) {
            try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {
                // 只要 is_done = 0 有数据，说明还没跑完
                try (ResultSet rs = stmt.executeQuery("SELECT * FROM system.mutations WHERE is_done = 0")) {
                    if (rs.next()) {
                        System.out.print("."); // 打印进度点
                        TimeUnit.SECONDS.sleep(10); // 等待5秒
                    } else {
                        System.out.println("\n>>> [OK] Mutation 队列已清空。准备进入下一个渠道。");
                        break; // 跳出 while，回到主流程的 next channel
                    }
                }
            }
        }
    }

    private static Connection getConnection() throws SQLException {
        return DriverManager.getConnection(CK_URL, CK_USER, CK_PASSWORD);
    }
}