package sqlservertockutil.不经过linux同步;

import java.sql.*;
import java.util.*;
import java.text.SimpleDateFormat;
import java.time.YearMonth;
import java.time.format.DateTimeFormatter;

/**
 * 可配置的SQLServer与ClickHouse数据对比工具
 * 支持自定义服务器、表映射和日期字段
 * 支持起始月和结束月的累计查询
 */
public class DataComparisonConfig {
    
    // SQLServer连接信息
    private static String sqlServerUser;
    private static String sqlServerPassword;
    
    // ClickHouse连接信息
    private static String clickhouseUrl;
    private static String clickhouseUser;
    private static String clickhousePassword;
    
    // 服务器表配置
    private static Map<String, Map<String, Integer>> serverTableMapping;
    
    // 映射关系
    private static Map<Integer, String> industryMapping;

    public static void main(String[] args) {
        Scanner scanner = new Scanner(System.in);
        
        // 初始化配置
        initConfiguration();
        
        System.out.println("=== SQLServer与ClickHouse数据对比工具（累计查询版本） ===");
        
        while (true) {
            // 获取起始月和结束月
            System.out.print("\n请输入起始月份(格式: YYYYMM，例如: 202401，输入quit退出): ");
            String startMonth = scanner.nextLine().trim();
            
            if ("quit".equalsIgnoreCase(startMonth)) {
                System.out.println("程序已退出！");
                break;
            }
            
            if (!isValidMonthFormat(startMonth)) {
                System.err.println("起始月份格式错误！请使用YYYYMM格式，例如: 202401");
                continue;
            }
            
            System.out.print("请输入结束月份(格式: YYYYMM，例如: 202504): ");
            String endMonth = scanner.nextLine().trim();
            
            if (!isValidMonthFormat(endMonth)) {
                System.err.println("结束月份格式错误！请使用YYYYMM格式，例如: 202504");
                continue;
            }
            
            // 验证日期范围
            if (!isValidDateRange(startMonth, endMonth)) {
                System.err.println("日期范围错误！结束月份必须大于或等于起始月份");
                continue;
        }
        
        System.out.print("请输入要检查的IndustryID（多个用逗号分隔，回车表示检查所有）: ");
        String industryInput = scanner.nextLine().trim();
        
        Set<Integer> targetIndustries = parseIndustryIds(industryInput);
        
                    System.out.println("\n开始对比 " + startMonth + " 到 " + endMonth + " 的累计数据...\n");
        
        // 按IndustryID收集所有服务器的表
        Map<Integer, Map<String, List<String>>> industryServerTables = new HashMap<>();
        
        // 遍历每个服务器，收集表信息
        for (Map.Entry<String, Map<String, Integer>> serverEntry : serverTableMapping.entrySet()) {
            String serverIp = serverEntry.getKey();
            Map<String, Integer> tables = serverEntry.getValue();
            
            System.out.println("==========================================");
            System.out.println("收集服务器: " + serverIp + " 的表信息");
            
            for (Map.Entry<String, Integer> tableEntry : tables.entrySet()) {
                String tableName = tableEntry.getKey();
                int industryId = tableEntry.getValue();
                
                // 如果指定了特定的industryId，进行过滤
                if (!targetIndustries.isEmpty() && !targetIndustries.contains(industryId)) {
                    continue;
                }
                
                // 按IndustryID分组，再按服务器分组
                industryServerTables.computeIfAbsent(industryId, k -> new HashMap<>())
                                  .computeIfAbsent(serverIp, k -> new ArrayList<>())
                                  .add(tableName);
                
                System.out.println("  收集表: " + tableName + " (IndustryID: " + industryId + ")");
            }
            System.out.println("==========================================\n");
        }
        
        // 按IndustryID进行全局对比
        compareByIndustryId(industryServerTables, startMonth, endMonth);
            
            System.out.println("本次数据对比完成！");
            System.out.println("提示：输入新的起始月份继续查询，或输入 quit 退出程序");
        }
        
        scanner.close();
    }
    
    /**
     * 初始化默认配置
     */
    private static void initConfiguration() {
        // 默认SQLServer配置
        sqlServerUser = "sa";
        sqlServerPassword = "smartpthdata";
        
        // 默认ClickHouse配置
        clickhouseUrl = "jdbc:clickhouse://hadoop110:8123/ods";
        clickhouseUser = "default";
        clickhousePassword = "smartpath";
        
        // 服务器表配置：服务器IP -> 表名 -> industryid
        serverTableMapping = new HashMap<String, Map<String, Integer>>() {{
            // 192.168.4.39 服务器的表
            put("192.168.4.39", new HashMap<String, Integer>() {{
                put("TradingContraceptionTY63", 22);
                put("tradingyaopinTY80", 55);
                put("tradingyaopinTY143", 55);
                put("tradingyaopinTY145", 55);
                put("tradingyaopinTY147", 55);
                put("tradingyaopinTY443", 55);
                put("tradingyaopinTY485", 55);
                put("tradingyaopinTY489", 55);
                put("tradingyaopinrxTY923", 56);
                put("tradingyingyangpinTY97", 59);
                put("tradingyingyangpinTY99", 59);
                put("tradingyingyangpinTY463", 59);
                put("tradingyingyangpinTY468", 59);
                put("tradingyingyangpinTY469", 59);
                put("tradingyingyangpinTY550", 59);
                put("tradingweishengsuTY200", 91);
                put("TradingNasalSalineTY375", 105);
                put("TradingContraception63", 22);
                put("tradingyaopin80", 55);
                put("tradingyaopin143", 55);
                put("tradingyaopin145", 55);
                put("tradingyaopin147", 55);
                put("tradingyaopin443", 55);
                put("tradingyaopin485", 55);
                put("tradingyaopin489", 55);
                put("tradingyaopinrx923", 56);
                put("tradingyingyangpin97", 59);
                put("tradingyingyangpin99", 59);
                put("tradingyingyangpin463", 59);
                put("tradingyingyangpin468", 59);
                put("tradingyingyangpin469", 59);
                put("tradingyingyangpin550", 59);
                put("tradingweishengsu200", 91);
                put("TradingNasalSaline375", 105);
            }});
            
            // 192.168.4.37 服务器的表
            put("192.168.4.37", new HashMap<String, Integer>() {{
                put("TradingPrivateCareTY68", 24);
                put("TradingPrivateCareTY69", 24);
                put("TradingPrivateCare68", 24);
                put("TradingPrivateCare69", 24);
                put("tradingyaopinTY142", 55);
                put("tradingyaopinTY146", 55);
                put("tradingyaopinTY440", 55);
                put("tradingyaopinTY490", 55);
                put("tradingyaopin142", 55);
                put("tradingyaopin146", 55);
                put("tradingyaopin440", 55);
                put("tradingyaopin490", 55);
                put("tradingyingyangpinTY94", 59);
                put("tradingyingyangpinTY95", 59);
                put("tradingyingyangpinTY208", 59);
                put("tradingyingyangpinTY460", 59);
                put("tradingyingyangpinTY467", 59);
                put("tradingyingyangpin94", 59);
                put("tradingyingyangpin95", 59);
                put("tradingyingyangpin208", 59);
                put("tradingyingyangpin460", 59);
                put("tradingyingyangpin467", 59);
                put("tradingshampooTY166", 86);
                put("tradingshampoo166", 86);
                put("TradingScarTY650", 195);
                put("TradingScar650", 195);

            }});
            
            // 192.168.4.38 服务器的表  
            put("192.168.4.38", new HashMap<String, Integer>() {{
                put("tradingyaopinTY139", 55);
                put("tradingyaopinTY140", 55);
                put("tradingyaopinTY141", 55);
                put("tradingyaopinTY144", 55);
                put("tradingyaopinTY486", 55);
                put("tradingyaopinTY487", 55);
                put("tradingyaopinTY488", 55);
                put("tradingyingyangpinTY96", 59);
                put("tradingyingyangpinTY167", 59);
                put("tradingyingyangpinTY209", 59);
                put("tradingyingyangpinTY464", 59);
                put("tradingyingyangpinTY470", 59);
                put("tradingyaopin139", 55);
                put("tradingyaopin140", 55);
                put("tradingyaopin141", 55);
                put("tradingyaopin144", 55);
                put("tradingyaopin486", 55);
                put("tradingyaopin487", 55);
                put("tradingyaopin488", 55);
                put("tradingyingyangpin96", 59);
                put("tradingyingyangpin167", 59);
                put("tradingyingyangpin209", 59);
                put("tradingyingyangpin464", 59);
                put("tradingyingyangpin470", 59);



            }});
        }};
        
        // 默认映射关系
        industryMapping = new HashMap<Integer, String>() {{
            put(22, "O2O_BYCP_22");
            put(24, "O2O_SCHL_24");
            put(55, "O2O_YP_55");
            put(91, "O2O_WSS_91");
            put(59, "O2O_CTZB_59");
            put(86, "O2O_XFS_86");
            put(195, "O2O_QBCP_195");
            put(56, "O2O_GMKSRX_56");
            put(105, "O2O_BQHL_105");
        }};
    }
    

    
    /**
     * 解析IndustryID输入
     */
    private static Set<Integer> parseIndustryIds(String input) {
        Set<Integer> result = new HashSet<>();
        if (input == null || input.trim().isEmpty()) {
            return result; // 空集合表示检查所有
        }
        
        String[] parts = input.split(",");
        for (String part : parts) {
            try {
                int id = Integer.parseInt(part.trim());
                result.add(id);
            } catch (NumberFormatException e) {
                System.out.println("警告: 无效的IndustryID - " + part);
            }
        }
        
        return result;
    }
    
    /**
     * 按IndustryID进行全局对比
     */
    private static void compareByIndustryId(Map<Integer, Map<String, List<String>>> industryServerTables, String startMonth, String endMonth) {
        System.out.println("🔍 开始按IndustryID进行全局数据对比...\n");
        
        Connection ckConnection = null;
        Map<String, Connection> sqlServerConnections = new HashMap<>();
        
        try {
            // 连接ClickHouse
            ckConnection = DriverManager.getConnection(clickhouseUrl, clickhouseUser, clickhousePassword);
            System.out.println("  ✅ ClickHouse连接成功");
            
            // 为每个服务器建立连接
            for (String serverIp : getAllServerIps(industryServerTables)) {
                String sqlServerUrl = "jdbc:sqlserver://" + serverIp + ":1433;database=trading_medicine";
                Connection conn = DriverManager.getConnection(sqlServerUrl, sqlServerUser, sqlServerPassword);
                sqlServerConnections.put(serverIp, conn);
                System.out.println("  ✅ SQLServer " + serverIp + " 连接成功");
            }
            
            System.out.println();
            
            // 遍历每个IndustryID
            for (Map.Entry<Integer, Map<String, List<String>>> industryEntry : industryServerTables.entrySet()) {
                int industryId = industryEntry.getKey();
                Map<String, List<String>> serverTables = industryEntry.getValue();
                
                System.out.println("🎯 ==========================================");
                System.out.println("🎯 IndustryID: " + industryId);
                System.out.println("🎯 ==========================================");
                
                // 获取对应的ClickHouse表名
                String ckTableName = industryMapping.get(industryId);
                if (ckTableName == null) {
                    System.out.println("    ❌ 未找到IndustryID " + industryId + " 对应的ClickHouse表映射");
                    continue;
                }
                
                // 累计所有服务器上该IndustryID的SQLServer数据
                long totalSqlServerCount = 0;
                int totalTables = 0;
                
                for (Map.Entry<String, List<String>> serverEntry : serverTables.entrySet()) {
                    String serverIp = serverEntry.getKey();
                    List<String> tableList = serverEntry.getValue();
                    
                    System.out.println("  📊 服务器: " + serverIp);
                    System.out.println("    包含表: " + tableList);
                    
                    Connection sqlConn = sqlServerConnections.get(serverIp);
                    long serverSqlCount = 0;
                    
                    for (String tableName : tableList) {
                        System.out.println("      检查表: " + tableName);
                        long tableCount = getSQLServerDataCountWithConditionConfig(sqlConn, tableName, startMonth, endMonth);
                        
                        if (tableCount == -1) {
                            System.out.println("        ❌ 表查询失败");
                            continue;
                        }
                        
                        serverSqlCount += tableCount;
                        System.out.println("        📈 数据量: " + tableCount);
                    }
                    
                    totalSqlServerCount += serverSqlCount;
                    totalTables += tableList.size();
                    System.out.println("    📊 服务器 " + serverIp + " 累计: " + serverSqlCount + " (来自" + tableList.size() + "个表)");
                }
                
                // 查询ClickHouse数据（只查一次）
                System.out.println("\n  🔍 查询ClickHouse...");
                long ckCount = getCKDataCountBySourceTable(ckConnection, "ods." + ckTableName, "IndustryID_" + industryId, startMonth, endMonth);
                
                // 最终对比
                System.out.println("\n  📊 最终对比结果:");
                System.out.println("    SQLServer总累计: " + totalSqlServerCount + " (来自" + totalTables + "个表，跨" + serverTables.size() + "个服务器)");
                System.out.println("    ClickHouse总量:  " + ckCount);
                
                if (totalSqlServerCount == ckCount) {
                    System.out.println("    ✅ IndustryID " + industryId + " 数据量完全一致！");
                } else {
                    long difference = Math.abs(totalSqlServerCount - ckCount);
                    System.out.println("    ❌ IndustryID " + industryId + " 数据量不一致");
                    System.out.println("    📊 差异: " + difference);
                    
                    if (ckCount == 0) {
                        System.out.println("    ⚠️  警告: ClickHouse中没有数据，可能未同步");
                    } else if (totalSqlServerCount > ckCount) {
                        System.out.println("    📈 SQLServer数据较多，可能有新数据未同步到ClickHouse");
                    } else {
                        System.out.println("    📉 ClickHouse数据较多，需要检查数据源");
                    }
                }
                
                System.out.println();
            }
            
        } catch (SQLException e) {
            System.err.println("  ❌ 数据库连接失败: " + e.getMessage());
        } finally {
            // 关闭所有连接
            closeConnection(ckConnection);
            for (Connection conn : sqlServerConnections.values()) {
                closeConnection(conn);
            }
        }
    }
    
    /**
     * 获取所有服务器IP列表
     */
    private static Set<String> getAllServerIps(Map<Integer, Map<String, List<String>>> industryServerTables) {
        Set<String> serverIps = new HashSet<>();
        for (Map<String, List<String>> serverTables : industryServerTables.values()) {
            serverIps.addAll(serverTables.keySet());
        }
        return serverIps;
    }
    

    
    /**
     * 从ClickHouse表中获取所有source_table
     */
    private static List<String> getSourceTablesFromCK(Connection connection, String tableName, String startMonth, String endMonth) throws SQLException {
        List<String> sourceTables = new ArrayList<>();
        String sql = "SELECT DISTINCT source_table FROM " + tableName + " WHERE pt_ym >= ? AND pt_ym <= ?";
        
        System.out.println("    获取source_table列表的SQL: " + sql);
        System.out.println("    参数: 起始月=" + startMonth + ", 结束月=" + endMonth);
        
        try (PreparedStatement stmt = connection.prepareStatement(sql)) {
            stmt.setString(1, startMonth);
            stmt.setString(2, endMonth);
            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    String sourceTable = rs.getString("source_table");
                    if (sourceTable != null && !sourceTable.trim().isEmpty()) {
                        sourceTables.add(sourceTable.trim());
                    }
                }
            }
        }
        
        return sourceTables;
    }
    
    /**
     * 获取ClickHouse表指定月份的总数据量
     */
    private static long getCKDataCount(Connection connection, String tableName, String startMonth, String endMonth) throws SQLException {
        String sql = "SELECT COUNT(*) as cnt FROM " + tableName + " WHERE pt_ym >= ? AND pt_ym <= ?";
        
        System.out.println("    ClickHouse总数据量查询SQL: " + sql);
        System.out.println("    参数: 起始月=" + startMonth + ", 结束月=" + endMonth);
        
        try (PreparedStatement stmt = connection.prepareStatement(sql)) {
            stmt.setString(1, startMonth);
            stmt.setString(2, endMonth);
            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    long count = rs.getLong("cnt");
                    System.out.println("    ClickHouse总数据量: " + count);
                    return count;
                }
            }
        }
        
        return 0;
    }
    
    /**
     * 获取ClickHouse表中指定月份的数据量
     */
    private static long getCKDataCountBySourceTable(Connection connection, String tableName, String sourceTable, String startMonth, String endMonth) throws SQLException {
        String sql = "SELECT COUNT(*) as cnt FROM " + tableName + " WHERE pt_ym >= ? AND pt_ym <= ?";
        
        System.out.println("    ClickHouse查询表: " + tableName);
        System.out.println("    ClickHouse查询SQL: " + sql);
        System.out.println("    参数: 起始月=" + startMonth + ", 结束月=" + endMonth + " (对应SQLServer表: " + sourceTable + ")");
        
        try (PreparedStatement stmt = connection.prepareStatement(sql)) {
            stmt.setString(1, startMonth);
            stmt.setString(2, endMonth);
            
            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    long count = rs.getLong("cnt");
                    System.out.println("    ClickHouse数据量: " + count);
                    return count;
                }
            }
        }
        
        return 0;
    }
    
    /**
     * 从SQLServer获取指定表和月份的数据量（配置版本，使用adddate字段）
     */
    private static long getSQLServerDataCountWithConditionConfig(Connection connection, String tableName, String startMonth, String endMonth) {
        // 将YYYYMM格式转换为日期范围
        String startDate = startMonth.substring(0, 4) + "-" + startMonth.substring(4, 6) + "-01 00:00:00.000";
        String endDate = getEndDateOfMonth(endMonth);

        System.out.println(startDate);
        System.out.println(endDate);
        
        try {
            // 检查表是否存在
            if (!tableExists(connection, tableName)) {
                return -1;
            }
            
            // 使用固定的adddate字段和categoryid IS NOT NULL条件
            String sql = "SELECT COUNT(*) as cnt FROM " + tableName + 
                        " WHERE adddate >= ? AND adddate < ? AND categoryid IS NOT NULL";


            
            try (PreparedStatement stmt = connection.prepareStatement(sql)) {
                stmt.setString(1, startDate);
                stmt.setString(2, endDate);
                
                try (ResultSet rs = stmt.executeQuery()) {
                    if (rs.next()) {
                        long count = rs.getLong("cnt");
                        System.out.println("    SQLServer数据量: " + count + " (使用adddate字段，条件: categoryid IS NOT NULL)");
                        return count;
                    }
                }
            }
            
        } catch (SQLException e) {
            System.out.println("    ❌ 查询失败: " + e.getMessage());
            return -1;
        }
        
        return -1;
    }


    
    /**
     * 检查表是否存在
     */
    private static boolean tableExists(Connection connection, String tableName) {
        try {
            DatabaseMetaData metaData = connection.getMetaData();
            try (ResultSet rs = metaData.getTables(null, null, tableName, new String[]{"TABLE"})) {
                return rs.next();
            }
        } catch (SQLException e) {
            return false;
        }
    }
    
    /**
     * 获取月份的结束日期
     */
    private static String getEndDateOfMonth(String targetMonth) {
        int year = Integer.parseInt(targetMonth.substring(0, 4));
        int month = Integer.parseInt(targetMonth.substring(4, 6));
        
        Calendar cal = Calendar.getInstance();
        // 重要：必须重置时分秒毫秒为0
        cal.set(year, month - 1, 1, 0, 0, 0); // 设置为当月第一天的00:00:00
        cal.set(Calendar.MILLISECOND, 0); // 重置毫秒为0
        cal.add(Calendar.MONTH, 1); // 加一个月，得到下个月第一天的00:00:00
        
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
        return sdf.format(cal.getTime());
    }
    
    /**
     * 验证月份格式
     */
    private static boolean isValidMonthFormat(String month) {
        if (month == null || month.length() != 6) {
            return false;
        }
        
        try {
            int year = Integer.parseInt(month.substring(0, 4));
            int monthValue = Integer.parseInt(month.substring(4, 6));
            return year >= 2000 && year <= 2100 && monthValue >= 1 && monthValue <= 12;
        } catch (NumberFormatException e) {
            return false;
        }
    }
    
    /**
     * 验证日期范围
     */
    private static boolean isValidDateRange(String startMonth, String endMonth) {
        YearMonth start = YearMonth.parse(startMonth, DateTimeFormatter.ofPattern("yyyyMM"));
        YearMonth end = YearMonth.parse(endMonth, DateTimeFormatter.ofPattern("yyyyMM"));
        return !start.isAfter(end);
    }
    
    /**
     * 生成起始月到结束月之间的所有月份列表
     */
    private static List<String> generateMonthRange(String startMonth, String endMonth) {
        List<String> months = new ArrayList<>();
        
        try {
            YearMonth start = YearMonth.parse(startMonth, DateTimeFormatter.ofPattern("yyyyMM"));
            YearMonth end = YearMonth.parse(endMonth, DateTimeFormatter.ofPattern("yyyyMM"));
            
            YearMonth current = start;
            while (!current.isAfter(end)) {
                months.add(current.format(DateTimeFormatter.ofPattern("yyyyMM")));
                current = current.plusMonths(1);
            }
        } catch (Exception e) {
            System.err.println("生成月份范围时出错: " + e.getMessage());
        }
        
        return months;
    }
    
    /**
     * 关闭数据库连接
     */
    private static void closeConnection(Connection connection) {
        if (connection != null) {
            try {
                connection.close();
            } catch (SQLException e) {
                System.err.println("关闭连接时出错: " + e.getMessage());
            }
        }
    }
} 