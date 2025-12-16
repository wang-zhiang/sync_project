package sqlservertoes;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.io.IOException;
import java.sql.*;
import java.util.*;
import java.util.Date;
import java.util.stream.Collectors;

public class check_data {
    private static final String SOURCE_DB_URL = "jdbc:sqlserver://192.168.4.219:1433;databaseName=online_dashboard_common";
    private static final String SOURCE_DB_USER = "sa";
    private static final String SOURCE_DB_PASSWORD = "smartpthdata";

    // 数据库凭证映射配置
    private static final Map<String, DbCredentials> DB_CREDENTIALS = new HashMap<>();
    static {
        DB_CREDENTIALS.put("192.168.4.218", new DbCredentials("CHH", "Y1v606"));
        DB_CREDENTIALS.put("192.168.4.201", new DbCredentials("CHH", "Y1v606"));
        DB_CREDENTIALS.put("192.168.4.212", new DbCredentials("CHH", "Y1v606"));
        DB_CREDENTIALS.put("192.168.4.35", new DbCredentials("CHH", "Y1v606"));
        DB_CREDENTIALS.put("192.168.4.36", new DbCredentials("CHH", "Y1v606"));
        DB_CREDENTIALS.put("192.168.4.40", new DbCredentials("sa", "smartpathdata"));
        DB_CREDENTIALS.put("192.168.4.41", new DbCredentials("sa", "smartpthdata"));
        DB_CREDENTIALS.put("192.168.4.57", new DbCredentials("sa", "smartpthdata"));
    }

    private RestHighLevelClient esClient;
    private Connection sourceConn;

    public static void main(String[] args) {
        System.out.println("[MAIN] 数据量检查程序启动");
        new check_data().startDataCheck();
    }

    private static void logInfo(String message) {
        System.out.println("[INFO] " + new Date() + " - " + message);
    }

    private static void logError(String message, Throwable t) {
        System.out.println("[ERROR] " + new Date() + " - " + message);
        if (t != null) {
            t.printStackTrace();
        }
    }

    public void startDataCheck() {
        logInfo("开始数据量检查流程");
        try {
            initializeConnections();
            List<SpCategorySql> sqlConfigs = loadSqlConfigs();
            logInfo("共加载 " + sqlConfigs.size() + " 条SQL配置");
            checkAllCategoriesDataCount(sqlConfigs);
        } catch (Exception e) {
            logError("数据检查过程中发生错误", e);
        } finally {
            closeConnections();
        }
        logInfo("数据量检查流程结束");
    }

    private void initializeConnections() throws SQLException {
        logInfo("初始化数据库连接...");
        try {
            Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
            logInfo("JDBC驱动加载成功");

            logInfo("尝试连接源数据库: " + SOURCE_DB_URL);
            sourceConn = DriverManager.getConnection(SOURCE_DB_URL, SOURCE_DB_USER, SOURCE_DB_PASSWORD);
            logInfo("源数据库连接成功");

        } catch (ClassNotFoundException e) {
            logError("找不到SQL Server驱动", e);
            throw new SQLException("JDBC驱动加载失败", e);
        } catch (SQLException e) {
            logError("数据库连接失败", e);
            throw e;
        }

        logInfo("初始化ES客户端...");
        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY,
                new UsernamePasswordCredentials("elastic", "smartpath"));

        esClient = new RestHighLevelClient(
                RestClient.builder(
                        new HttpHost("192.168.6.105", 9205, "http"),
                        new HttpHost("192.168.6.106", 9206, "http")
                ).setHttpClientConfigCallback(httpClientBuilder -> httpClientBuilder
                        .setDefaultCredentialsProvider(credentialsProvider))
        );
        logInfo("ES客户端初始化完成");
    }

    private List<SpCategorySql> loadSqlConfigs() throws SQLException {
        logInfo("开始加载SQL配置");
        List<SpCategorySql> configs = new ArrayList<>();

        String sql = "SELECT Id, SpCategoryId, ChannelType, DbHost, DbPort, DbName, SqlContent  FROM CL_Common_SpCategorySql where  SpCategoryId  = 109   ";

        try (Statement stmt = sourceConn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            while (rs.next()) {
                String sqlContent = rs.getString("SqlContent");
                if (sqlContent == null || sqlContent.trim().isEmpty()) {
                    logInfo("跳过空SQL内容的配置项，SpCategoryId: " + rs.getInt("SpCategoryId"));
                    continue;
                }

                SpCategorySql config = new SpCategorySql();
                config.setId(rs.getInt("Id"));
                config.setSpCategoryId(rs.getInt("SpCategoryId"));
                config.setChannelType(rs.getInt("ChannelType"));
                config.setDbHost(rs.getString("DbHost"));
                config.setDbPort(rs.getString("DbPort"));
                config.setDbName(rs.getString("DbName"));
                config.setSqlContent(sqlContent);
                configs.add(config);

                logInfo("加载配置: ID=" + config.getId() +
                        ", CategoryID=" + config.getSpCategoryId() +
                        ", Channel=" + config.getChannelType());
            }
        }
        return configs;
    }

    private void checkAllCategoriesDataCount(List<SpCategorySql> configs) {
        logInfo("开始检查所有分类数据量");
        Map<Integer, List<SpCategorySql>> grouped = configs.stream()
                .collect(Collectors.groupingBy(SpCategorySql::getSpCategoryId));

        logInfo("共发现 " + grouped.size() + " 个分类需要检查");
        
        // 统计结果
        List<DataCountResult> results = new ArrayList<>();
        
        grouped.forEach((categoryId, sqlConfigs) -> {
            try {
                logInfo(">>> 开始检查分类: " + categoryId);
                DataCountResult result = checkCategoryDataCount(categoryId, sqlConfigs);
                results.add(result);
                logInfo("<<< 完成检查分类: " + categoryId);
            } catch (Exception e) {
                logError("检查分类 " + categoryId + " 失败", e);
                DataCountResult errorResult = new DataCountResult();
                errorResult.categoryId = categoryId;
                errorResult.sqlServerTotalCount = -1;
                errorResult.esCount = -1;
                errorResult.isMatch = false;
                errorResult.errorMessage = e.getMessage();
                results.add(errorResult);
            }
        });
        
        // 输出汇总报告
        printSummaryReport(results);
    }

    private DataCountResult checkCategoryDataCount(int categoryId, List<SpCategorySql> sqlConfigs) throws Exception {
        SpCategory category = loadSpCategory(categoryId);
        DataCountResult result = new DataCountResult();
        result.categoryId = categoryId;
        result.category3Name = category.getCategory3();
        
        // 统计每个SQL查询的数据量
        long totalSqlCount = 0;
        for (SpCategorySql config : sqlConfigs) {
            try (Connection targetConn = getTargetConnection(config)) {
                long count = countSqlData(targetConn, config);
                totalSqlCount += count;
                logInfo("SQL查询 (渠道" + config.getChannelType() + ") 数据量: " + count);
            }
        }
        result.sqlServerTotalCount = totalSqlCount;
        
        // 获取ES索引数据量
        String indexName = category.getEsIndexName();
        if (indexName != null && !indexName.trim().isEmpty()) {
            result.esCount = countEsData(indexName);
        } else {
            result.esCount = 0;
            logInfo("ES索引名为空");
        }
        
        result.isMatch = (result.sqlServerTotalCount == result.esCount);
        logInfo("SQL Server总数据量: " + result.sqlServerTotalCount + ", ES数据量: " + result.esCount);
        
        return result;
    }
    
    private long countSqlData(Connection conn, SpCategorySql config) throws SQLException {
        String sql = config.getSqlContent();
        if (sql == null || sql.trim().isEmpty()) {
            return 0;
        }
        
        // 清理SQL：去除末尾的分号和空格
        sql = sql.trim();
        if (sql.endsWith(";")) {
            sql = sql.substring(0, sql.length() - 1).trim();
        }
        
        // 首先尝试子查询方法
        String countSql = "SELECT COUNT(*) FROM (" + sql + ") AS subquery";
        
        try (PreparedStatement pstmt = conn.prepareStatement(countSql);
             ResultSet rs = pstmt.executeQuery()) {
            if (rs.next()) {
                return rs.getLong(1);
            }
        } catch (SQLException e) {
            // 如果子查询失败（例如重复列名），则使用备用方法
            logInfo("子查询方法失败，尝试直接执行查询计数: " + e.getMessage());
            return countSqlDataDirect(conn, sql);
        }
        return 0;
    }
    
    private long countSqlDataDirect(Connection conn, String sql) throws SQLException {
        // 直接执行原始SQL并计算结果集行数
        long count = 0;
        try (PreparedStatement pstmt = conn.prepareStatement(sql);
             ResultSet rs = pstmt.executeQuery()) {
            while (rs.next()) {
                count++;
            }
            logInfo("直接计数方法完成，总行数: " + count);
            return count;
        } catch (SQLException e) {
            logError("直接执行SQL计数失败: " + e.getMessage(), e);
            throw e;
        }
    }
    
    private long countEsData(String indexName) {
        try {
            SearchRequest searchRequest = new SearchRequest(indexName);
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
            searchSourceBuilder.size(0); // 只获取总数，不获取文档
            searchSourceBuilder.trackTotalHits(true); // 关键！追踪真实总数
            searchRequest.source(searchSourceBuilder);
            
            SearchResponse searchResponse = esClient.search(searchRequest, RequestOptions.DEFAULT);
            return searchResponse.getHits().getTotalHits().value;
        } catch (Exception e) {
            logError("查询ES索引数据量失败: " + indexName, e);
            return -1;
        }
    }

    private Connection getTargetConnection(SpCategorySql config) throws SQLException {
        String port = (config.getDbPort() == null || config.getDbPort().trim().isEmpty())
                ? "1433" : config.getDbPort();
        String url = String.format("jdbc:sqlserver://%s:%s;databaseName=%s",
                config.getDbHost(), port, config.getDbName());

        DbCredentials credentials = DB_CREDENTIALS.getOrDefault(config.getDbHost(),
                new DbCredentials(SOURCE_DB_USER, SOURCE_DB_PASSWORD));

        logInfo("连接到数据库 " + config.getDbHost() + ":" + config.getDbPort() +
                "，使用用户: " + credentials.username);

        return DriverManager.getConnection(url, credentials.username, credentials.password);
    }

    private SpCategory loadSpCategory(int categoryId) throws SQLException {
        logInfo("加载分类信息，ID: " + categoryId);
        String sql = "SELECT * FROM CL_Common_SpCategory WHERE Id = ?";
        try (PreparedStatement pstmt = sourceConn.prepareStatement(sql)) {
            pstmt.setInt(1, categoryId);
            ResultSet rs = pstmt.executeQuery();
            if (rs.next()) {
                SpCategory category = new SpCategory();
                category.setId(categoryId);
                category.setCategory1(rs.getString("Category1"));
                category.setCategory2(rs.getString("Category2"));
                category.setCategory3(rs.getString("Category3"));
                category.setDataLevel(rs.getInt("DataLevel"));
                category.setEsIndexName(rs.getString("EsIndexName")); // 新增
                return category;
            }
            throw new SQLException("未找到分类: " + categoryId);
        }
    }

    private void printSummaryReport(List<DataCountResult> results) {
        logInfo("===========================================");
        logInfo("                数据量检查汇总报告");
        logInfo("===========================================");
        
        int totalCategories = results.size();
        int matchedCategories = 0;
        int unmatchedCategories = 0;
        int errorCategories = 0;
        
        // 分类汇总统计
        List<DataCountResult> matchedList = new ArrayList<>();
        List<DataCountResult> unmatchedList = new ArrayList<>();
        List<DataCountResult> errorList = new ArrayList<>();
        
        for (DataCountResult result : results) {
            if (result.errorMessage != null) {
                errorCategories++;
                errorList.add(result);
            } else if (result.isMatch) {
                matchedCategories++;
                matchedList.add(result);
            } else {
                unmatchedCategories++;
                unmatchedList.add(result);
            }
        }
        
        logInfo("总分类数: " + totalCategories);
        logInfo("数据一致分类数: " + matchedCategories);
        logInfo("数据不一致分类数: " + unmatchedCategories);
        logInfo("检查失败分类数: " + errorCategories);
        
        // 重点显示数据不一致的分类
        if (unmatchedCategories > 0) {
            logInfo("");
            logInfo("!!! 数据量不一致的分类详情 !!!");
            logInfo("-------------------------------------------");
            for (DataCountResult result : unmatchedList) {
                long difference = result.sqlServerTotalCount - result.esCount;
                String diffInfo = difference > 0 ? 
                    String.format("SQL多了 %d 条", difference) : 
                    String.format("ES多了 %d 条", Math.abs(difference));
                
                logInfo(String.format("【不一致】分类 %d (%s):",
                        result.categoryId,
                        result.category3Name != null ? result.category3Name : "未知"));
                logInfo(String.format("  SQL Server: %,d 条", result.sqlServerTotalCount));
                logInfo(String.format("  Elasticsearch: %,d 条", result.esCount));
                logInfo(String.format("  差异: %s", diffInfo));
                logInfo("");
            }
        }
        
        // 显示检查失败的分类
        if (errorCategories > 0) {
            logInfo("!!! 检查失败的分类 !!!");
            logInfo("-------------------------------------------");
            for (DataCountResult result : errorList) {
                logInfo(String.format("【错误】分类 %d (%s): %s",
                        result.categoryId,
                        result.category3Name != null ? result.category3Name : "未知",
                        result.errorMessage));
            }
            logInfo("");
        }
        
        // 显示数据一致的分类（简要信息）
        if (matchedCategories > 0) {
            logInfo("数据一致的分类:");
            logInfo("-------------------------------------------");
            for (DataCountResult result : matchedList) {
                logInfo(String.format("【一致】分类 %d (%s): %,d 条",
                        result.categoryId,
                        result.category3Name != null ? result.category3Name : "未知",
                        result.sqlServerTotalCount));
            }
        }
        
        logInfo("===========================================");
    }

    private void closeConnections() {
        try {
            if (sourceConn != null && !sourceConn.isClosed()) {
                sourceConn.close();
                logInfo("已关闭源数据库连接");
            }
        } catch (SQLException e) {
            logError("关闭源数据库连接时出错", e);
        }

        try {
            if (esClient != null) {
                esClient.close();
                logInfo("已关闭ES客户端");
            }
        } catch (Exception e) {
            logError("关闭ES客户端时出错", e);
        }
    }

    // 数据统计结果类
    private static class DataCountResult {
        int categoryId;
        String category3Name;
        long sqlServerTotalCount;
        long esCount;
        boolean isMatch;
        String errorMessage;
    }

    // 数据模型类
    private static class SpCategorySql {
        private int id;
        private int spCategoryId;
        private int channelType;
        private String dbHost;
        private String dbPort;
        private String dbName;
        private String sqlContent;

        // getters and setters
        public int getId() { return id; }
        public void setId(int id) { this.id = id; }
        public int getSpCategoryId() { return spCategoryId; }
        public void setSpCategoryId(int spCategoryId) { this.spCategoryId = spCategoryId; }
        public int getChannelType() { return channelType; }
        public void setChannelType(int channelType) { this.channelType = channelType; }
        public String getDbHost() { return dbHost; }
        public void setDbHost(String dbHost) { this.dbHost = dbHost; }
        public String getDbPort() { return dbPort; }
        public void setDbPort(String dbPort) { this.dbPort = dbPort; }
        public String getDbName() { return dbName; }
        public void setDbName(String dbName) { this.dbName = dbName; }
        public String getSqlContent() { return sqlContent; }
        public void setSqlContent(String sqlContent) { this.sqlContent = sqlContent; }
    }

    private static class SpCategory {
        private int id;
        private String category1;
        private String category2;
        private String category3;
        private int dataLevel;
        private String esIndexName;

        // getters and setters
        public int getId() { return id; }
        public void setId(int id) { this.id = id; }
        public String getCategory1() { return category1; }
        public void setCategory1(String category1) { this.category1 = category1; }
        public String getCategory2() { return category2; }
        public void setCategory2(String category2) { this.category2 = category2; }
        public String getCategory3() { return category3; }
        public void setCategory3(String category3) { this.category3 = category3; }
        public int getDataLevel() { return dataLevel; }
        public void setDataLevel(int dataLevel) { this.dataLevel = dataLevel; }
        public String getEsIndexName() { return esIndexName; }
        public void setEsIndexName(String esIndexName) { this.esIndexName = esIndexName; }
    }

    private static class DbCredentials {
        private String username;
        private String password;

        public DbCredentials(String username, String password) {
            this.username = username;
            this.password = password;
        }
    }
}