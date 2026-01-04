package o2o;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

//测试  洋酒 葡萄酒   数据量对比

public class TableCompareTool {

    // ================= 1. 数据库连接配置 (固定写好) =================
    // SQL Server 地址 1 (o2o)
    static final String DB_URL_O2O = "jdbc:sqlserver://192.168.4.39;DatabaseName=trading_medicine;encrypt=false";
    // SQL Server 地址 2 (trading_hm)
    static final String DB_URL_HM = "jdbc:sqlserver://192.168.4.99:1444;DatabaseName=trading_hm;encrypt=false";

    static final String SS_USER = "sa";
    static final String SS_PASS = "smartpthdata";

    // ClickHouse 地址
    static final String CK_URL = "jdbc:clickhouse://192.168.5.110:8123";
    static final String CK_USER = "default";
    static final String CK_PASS = "smartpath";

    public static void main(String[] args) {
        // 总任务列表
        List<CompareTask> allTasks = new ArrayList<>();

        // =========================================================================
        //                          TODO: 配置区域 (核心修改区)
        // =========================================================================

        // --- 第 1 组：MT/O2O 的表 (共用一套配置) ---
        // 1.1 把所有属于这一类的表名，都写在 Arrays.asList 里，用逗号隔开
        List<String> mtTables = Arrays.asList(
                "tradingfoodyangjiuhy1482023",
                "tradingfoodyangjiuhy1482024",
                "tradingfoodyangjiuty148",
                "tradingfoodyangjiu148"
        );

        // 1.2 批量添加任务 (MT组)
        addBatchTasks(allTasks, mtTables,
                DB_URL_O2O,                                      // 这一组用的数据库URL
                "sellerid = '2222' AND categoryid is not null",  // 这一组统一的 SQL Server 条件
                "ods.elmoriginaldetail",                          // 这一组统一对比的 CK 表
                "IndustryId = 73"                                // 这一组统一的 CK 条件
        );


        // --- 第 2 组：HM 的表 (共用一套配置) ---
        // 2.1 把 HM 的表名写在这里
        List<String> hmTables = Arrays.asList(
                "tradingfoodyangjiu"
        );

        // 2.2 批量添加任务 (HM组)
        addBatchTasks(allTasks, hmTables,
                DB_URL_HM,                                       // 注意：这里切换成了 HM 的数据库
                "categoryid is not null and  adddate >= '2023-01-01 00:00:00.000'",                        // 注意：这里条件变了 (没有sellerid了)
                "ods.hmoriginaldetail",                          // 对比的 CK 表也变了
                "IndustryId = 73"                                // CK 条件
        );

        // =========================================================================
        //                          配置结束，开始执行
        // =========================================================================

        System.out.println("共生成了 " + allTasks.size() + " 个对比任务，开始执行...");
        for (CompareTask task : allTasks) {
            runCompare(task);
        }
    }

    /**
     * 辅助方法：批量创建任务，避免重复写代码
     */
    public static void addBatchTasks(List<CompareTask> targetList, List<String> tableNames,
                                     String ssUrl, String ssCond, String ckTable, String ckCond) {
        for (String tableName : tableNames) {
            // 任务名直接用表名
            targetList.add(new CompareTask(tableName, ssUrl, tableName, ssCond, ckTable, ckCond));
        }
    }

    // --------------- 下面是核心逻辑 (已包含 FORMAT 兼容性修复) ------------------

    public static void runCompare(CompareTask task) {
        System.out.println("\n################################################################");
        System.out.println("正在执行任务: [" + task.taskName + "]");
        System.out.println("SQLServer表: " + task.ssTableName + " (库: " + (task.ssUrl.contains("hm") ? "HM" : "O2O") + ")");
        System.out.println("ClickHouse表: " + task.ckTableName);
        System.out.println("################################################################");

        Map<String, DataRow> ssMap = querySqlServer(task);
        if (ssMap.isEmpty()) {
            System.out.println("SQL Server 未查询到任何数据，跳过对比。");
            return;
        }

        Map<String, DataRow> ckMap = queryClickHouse(task);

        System.out.printf("%-10s | %-15s | %-15s | %-10s%n", "月份", "数量差(SS-CK)", "金额差(SS-CK)", "结果");
        System.out.println("-------------------------------------------------------------");

        for (String month : ssMap.keySet()) {
            DataRow ssRow = ssMap.get(month);
            DataRow ckRow = ckMap.getOrDefault(month, new DataRow(0, BigDecimal.ZERO));

            long countDiff = ssRow.count - ckRow.count;
            BigDecimal valDiff = ssRow.sumVal.subtract(ckRow.sumVal);

            boolean ok = (countDiff == 0) && (valDiff.compareTo(BigDecimal.ZERO) == 0);
            String status = ok ? "✅ 通过" : "❌ 异常";

            System.out.printf("%-10s | %-15d | %-15s | %s%n",
                    month, countDiff, valDiff.toPlainString(), status);

            if (!ok) {
                System.out.println("   >>> 详情: SS[数量:" + ssRow.count + ", 金额:" + ssRow.sumVal + "]  vs  CK[数量:" + ckRow.count + ", 金额:" + ckRow.sumVal + "]");
            }
        }
    }

    private static Map<String, DataRow> querySqlServer(CompareTask task) {
        Map<String, DataRow> result = new HashMap<>();
        String sqlWhere = (task.ssCondition == null || task.ssCondition.isEmpty()) ? "" : " WHERE " + task.ssCondition;

        // 使用兼容写法: LEFT(CONVERT(varchar, adddate, 112), 6)
        String sql = "SELECT LEFT(CONVERT(varchar, adddate, 112), 6) as pt_ym, COUNT(*) as cnt, SUM(price*qty) as sum_val " +
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

    private static Map<String, DataRow> queryClickHouse(CompareTask task) {
        Map<String, DataRow> result = new HashMap<>();
        String sqlWhere = (task.ckCondition == null || task.ckCondition.isEmpty()) ? "" : " WHERE " + task.ckCondition;

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

    static class CompareTask {
        String taskName;
        String ssUrl;
        String ssTableName;
        String ssCondition;
        String ckTableName;
        String ckCondition;

        public CompareTask(String name, String url, String ssTable, String ssCond, String ckTable, String ckCond) {
            this.taskName = name;
            this.ssUrl = url;
            this.ssTableName = ssTable;
            this.ssCondition = ssCond;
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