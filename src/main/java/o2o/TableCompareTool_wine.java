package o2o;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.*;

// 版本 V6：JDDJ 增加 ttype=71 条件
public class TableCompareTool_wine {

    // =========================================================================
    //                          1. 数据库配置区
    // =========================================================================

    static final String SS_USER = "sa";
    static final String SS_PASS = "smartpthdata";
    static final String CK_USER = "default";
    static final String CK_PASS = "smartpath";

    // SQL Server 地址
    static final String DB_URL_O2O = "jdbc:sqlserver://192.168.4.39;DatabaseName=trading_medicine;encrypt=false";
    static final String DB_URL_HM = "jdbc:sqlserver://192.168.4.99:1444;DatabaseName=trading_hm;encrypt=false";
    static final String DB_URL_JDDJ = "jdbc:sqlserver://192.168.4.39;DatabaseName=trading_medicinenew;encrypt=false";

    // ClickHouse 地址
    static final String CK_URL = "jdbc:clickhouse://192.168.5.110:8123";

    // 公共过滤条件
    static final String WINE_CATEGORY_FILTER = "categoryid in (10,20,30,40,50)";
    static final String CK_INDUSTRY_FILTER = "IndustryId = 71";


    public static void main(String[] args) {
        Scanner scanner = new Scanner(System.in);

        System.out.println("============================================================");
        System.out.println("      葡萄酒数据对比工具 (V6 - JDDJ ttype=71版)");
        System.out.println("============================================================");

        // 1. 获取时间范围输入
        System.out.print("请输入 [开始月份] (格式 yyyyMM, 例 202501): ");
        String startYm = scanner.next().trim();

        System.out.print("请输入 [结束月份] (格式 yyyyMM, 例 202512): ");
        String endYm = scanner.next().trim();

        System.out.println("\n>>> 对比区间: " + startYm + " 至 " + endYm);
        System.out.println(">>> 正在生成任务列表...\n");

        List<WineTask> allTasks = new ArrayList<>();

        // =========================================================================
        //                          2. 任务配置区
        // =========================================================================

        // --- 第 1 组：MT/O2O 的表 (字段用 qty) ---
        List<String> mtTables = Arrays.asList(
                "tradingfoodjiuhy1492023",
                "tradingfoodjiuhy1492024",
                "tradingfoodjiuty149",
                "tradingfoodjiu149"
        );
        addBatchTasks(allTasks, mtTables,
                DB_URL_O2O,
                "sellerid = '2222' AND categoryid is not null",
                "qty",
                "ods.elmoriginaldetail",
                CK_INDUSTRY_FILTER,
                startYm, endYm
        );


        // --- 第 2 组：HM 的表 (字段用 qty) ---
        List<String> hmTables = Arrays.asList(
                "tradingfoodjiu"
        );
        addBatchTasks(allTasks, hmTables,
                DB_URL_HM,
                WINE_CATEGORY_FILTER,
                "qty",
                "ods.hmoriginaldetail",
                CK_INDUSTRY_FILTER,
                startYm, endYm
        );


        // --- 第 3 组：京东到家 (JDDJ) 的表 (字段用 monthSales) ---
        List<String> jddjTables = Arrays.asList(
                "tradingjddj"
        );
        addBatchTasks(allTasks, jddjTables,
                DB_URL_JDDJ,
                // ★★★ 修改点：在这里追加了 AND ttype = 71 ★★★
                WINE_CATEGORY_FILTER + " AND ttype = 71",
                "monthSales",
                "ods.jddjoriginaldetail",
                CK_INDUSTRY_FILTER,
                startYm, endYm
        );

        // =========================================================================
        //                          执行逻辑
        // =========================================================================

        System.out.println("共生成了 " + allTasks.size() + " 个对比任务，开始执行...");
        for (WineTask task : allTasks) {
            runCompare(task);
        }

        System.out.println("\n全部任务执行完毕。");
        scanner.close();
    }

    /**
     * 辅助方法：批量创建任务
     */
    public static void addBatchTasks(List<WineTask> targetList, List<String> tableNames,
                                     String ssUrl, String ssBaseCond, String qtyColumn,
                                     String ckTable, String ckBaseCond,
                                     String startYm, String endYm) {
        for (String tableName : tableNames) {
            // 拼接 SQL Server 条件
            String finalSsCond = ssBaseCond;
            if (!finalSsCond.isEmpty()) finalSsCond += " AND ";
            finalSsCond += "LEFT(CONVERT(varchar, adddate, 112), 6) BETWEEN '" + startYm + "' AND '" + endYm + "'";

            // 拼接 ClickHouse 条件
            String finalCkCond = ckBaseCond;
            if (!finalCkCond.isEmpty()) finalCkCond += " AND ";
            finalCkCond += "pt_ym >= '" + startYm + "' AND pt_ym <= '" + endYm + "'";

            targetList.add(new WineTask(tableName, ssUrl, tableName, finalSsCond, qtyColumn, ckTable, finalCkCond));
        }
    }

    public static void runCompare(WineTask task) {
        System.out.println("\n########################################################################################");
        System.out.println("任务: [" + task.taskName + "] | DB: " + formatDbName(task.ssUrl));
        System.out.println("SS计算公式: SUM(price * " + task.ssQtyColumn + ")");
        System.out.println("SS筛选条件: " + task.ssCondition);
        System.out.println("########################################################################################");

        Map<String, DataRow> ssMap = querySqlServer(task);
        if (ssMap.isEmpty()) {
            System.out.println(">>> 警告: SQL Server 未查询到任何数据，跳过。");
            return;
        }

        Map<String, DataRow> ckMap = queryClickHouse(task);

        // 打印表头
        System.out.printf("%-8s | %-10s | %-10s | %-10s || %-15s | %-15s | %-15s | %-8s%n",
                "月份", "SS数量", "CK数量", "数量差", "SS金额", "CK金额", "金额差", "结果");
        System.out.println("----------------------------------------------------------------------------------------------------------------");

        Set<String> allMonths = new TreeSet<>(ssMap.keySet());
        allMonths.addAll(ckMap.keySet());

        for (String month : allMonths) {
            DataRow ssRow = ssMap.getOrDefault(month, new DataRow(0, BigDecimal.ZERO));
            DataRow ckRow = ckMap.getOrDefault(month, new DataRow(0, BigDecimal.ZERO));

            long countDiff = ssRow.count - ckRow.count;
            BigDecimal valDiff = ssRow.sumVal.subtract(ckRow.sumVal);

            boolean ok = (countDiff == 0) && (valDiff.compareTo(BigDecimal.ZERO) == 0);
            String status = ok ? "✅" : "❌";

            // 格式化输出
            System.out.printf("%-8s | %-10d | %-10d | %-10d || %-15s | %-15s | %-15s | %s%n",
                    month,
                    ssRow.count, ckRow.count, countDiff,
                    ssRow.sumVal.toPlainString(), ckRow.sumVal.toPlainString(), valDiff.toPlainString(),
                    status);
        }
    }

    private static Map<String, DataRow> querySqlServer(WineTask task) {
        Map<String, DataRow> result = new HashMap<>();
        String sqlWhere = (task.ssCondition == null || task.ssCondition.isEmpty()) ? "" : " WHERE " + task.ssCondition;

        // 动态替换数量字段: price * qty 或 price * monthSales
        String calcFormula = "price * " + task.ssQtyColumn;

        String sql = "SELECT LEFT(CONVERT(varchar, adddate, 112), 6) as pt_ym, COUNT(*) as cnt, SUM(" + calcFormula + ") as sum_val " +
                "FROM " + task.ssTableName + sqlWhere +
                " GROUP BY LEFT(CONVERT(varchar, adddate, 112), 6)";

        try (Connection conn = DriverManager.getConnection(task.ssUrl, SS_USER, SS_PASS);
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            while (rs.next()) {
                String ym = rs.getString("pt_ym");
                if (ym == null) continue;
                long cnt = rs.getLong("cnt");
                BigDecimal val = rs.getBigDecimal("sum_val");
                if (val == null) val = BigDecimal.ZERO;
                val = val.setScale(4, RoundingMode.HALF_UP);
                result.put(ym, new DataRow(cnt, val));
            }
        } catch (Exception e) {
            System.err.println("查询 SQL Server 出错: " + e.getMessage());
            System.err.println("出错 SQL: " + sql);
        }
        return result;
    }

    private static Map<String, DataRow> queryClickHouse(WineTask task) {
        Map<String, DataRow> result = new HashMap<>();
        String sqlWhere = (task.ckCondition == null || task.ckCondition.isEmpty()) ? "" : " WHERE " + task.ckCondition;

        // CK 保持不变，始终 SUM(Val)
        String sql = "SELECT pt_ym, COUNT() as cnt, SUM(Val) as sum_val " +
                "FROM " + task.ckTableName + sqlWhere +
                " GROUP BY pt_ym";

        try (Connection conn = DriverManager.getConnection(CK_URL, CK_USER, CK_PASS);
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            while (rs.next()) {
                String ym = rs.getString("pt_ym");
                long cnt = rs.getLong("cnt");
                BigDecimal val = rs.getBigDecimal("sum_val");
                if (val == null) val = BigDecimal.ZERO;
                val = val.setScale(4, RoundingMode.HALF_UP);
                result.put(ym, new DataRow(cnt, val));
            }
        } catch (Exception e) {
            System.err.println("查询 ClickHouse 出错: " + e.getMessage());
        }
        return result;
    }

    private static String formatDbName(String url) {
        if (url.contains("trading_medicinenew")) return "JDDJ";
        if (url.contains("trading_hm")) return "HM";
        if (url.contains("trading_medicine")) return "MT/O2O";
        return "Unknown";
    }

    static class WineTask {
        String taskName;
        String ssUrl;
        String ssTableName;
        String ssCondition;
        String ssQtyColumn;
        String ckTableName;
        String ckCondition;

        public WineTask(String name, String url, String ssTable, String ssCond, String qtyCol, String ckTable, String ckCond) {
            this.taskName = name;
            this.ssUrl = url;
            this.ssTableName = ssTable;
            this.ssCondition = ssCond;
            this.ssQtyColumn = qtyCol;
            this.ckTableName = ckTable;
            this.ckCondition = ckCond;
        }
    }

    static class DataRow {
        long count;
        BigDecimal sumVal;
        public DataRow(long c, BigDecimal v) { this.count = c; this.sumVal = v; }
    }
}