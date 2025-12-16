package sqlservertoes;
import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class checksql {
    private static final String SOURCE_DB_URL = "jdbc:sqlserver://192.168.4.219:1433;databaseName=online_dashboard_common";
    private static final String SOURCE_DB_USER = "sa";
    private static final String SOURCE_DB_PASSWORD = "smartpthdata";

    // 数据库凭证映射配置（与原代码一致）
    private static final Map<String, DbCredentials> DB_CREDENTIALS = new HashMap<>();
    static {
        DB_CREDENTIALS.put("192.168.4.218", new DbCredentials("CHH", "Y1v606"));
        DB_CREDENTIALS.put("192.168.4.201", new DbCredentials("CHH", "Y1v606"));
        DB_CREDENTIALS.put("192.168.4.212", new DbCredentials("CHH", "Y1v606"));
        DB_CREDENTIALS.put("192.168.4.35", new DbCredentials("CHH", "Y1v606"));
        DB_CREDENTIALS.put("192.168.4.36", new DbCredentials("CHH", "Y1v606"));
    }

    public static void main(String[] args) {
        List<EmptySqlResult> emptyResults = checkEmptySqls();
        System.out.println("以下 SQL 执行无数据：");
        emptyResults.forEach(result ->
                System.out.printf("SpCategoryId: %d, ChannelType: %d%n",
                        result.spCategoryId, result.channelType)
        );
    }

    public static List<EmptySqlResult> checkEmptySqls() {
        List<EmptySqlResult> emptyResults = new ArrayList<>();
        try (Connection sourceConn = DriverManager.getConnection(SOURCE_DB_URL, SOURCE_DB_USER, SOURCE_DB_PASSWORD)) {
            // 1. 加载所有 SQL 配置
            List<SpCategorySql> sqlConfigs = loadAllSqlConfigs(sourceConn);

            // 2. 逐个检查 SQL 是否有数据
            for (SpCategorySql config : sqlConfigs) {
                if (isSqlEmpty(config)) {
                    emptyResults.add(new EmptySqlResult(
                            config.getSpCategoryId(),
                            config.getChannelType()
                    ));
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return emptyResults;
    }

    private static List<SpCategorySql> loadAllSqlConfigs(Connection conn) throws SQLException {
        List<SpCategorySql> configs = new ArrayList<>();
        String sql = "SELECT SpCategoryId, ChannelType, DbHost, DbPort, DbName, SqlContent FROM CL_Common_SpCategorySql where ChannelType in (1,2) and SpCategoryId  in (100,102,103,104,105,108,109,110,111,112,113,114,123,125,128,129,130,133,134,135,118,119,122,124,126,127,131,132,136,137,138,31,32,33,47,48,49,50,53,56,63,66,67,68,69,70,72,73,75,54,76,78,79,82,83,84,85,87,92,93,121,141,5,10,11,29,46,51,55,57,58,59);";

        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            while (rs.next()) {
                SpCategorySql config = new SpCategorySql();
                config.setSpCategoryId(rs.getInt("SpCategoryId"));
                config.setChannelType(rs.getInt("ChannelType"));
                config.setDbHost(rs.getString("DbHost"));
                config.setDbPort(rs.getString("DbPort"));
                config.setDbName(rs.getString("DbName"));
                config.setSqlContent(rs.getString("SqlContent"));
                configs.add(config);
            }
        }
        return configs;
    }

    private static boolean isSqlEmpty(SpCategorySql config) {
        String sql = config.getSqlContent();
        if (sql == null || sql.trim().isEmpty()) return true; // 空 SQL 直接视为无数据

        try (Connection targetConn = getTargetConnection(config);
             PreparedStatement pstmt = targetConn.prepareStatement(sql);
             ResultSet rs = pstmt.executeQuery()) {
            return !rs.next(); // 关键判断：无数据返回 true
        } catch (SQLException e) {
            System.err.printf("执行 SQL 失败: SpCategoryId=%d, ChannelType=%d%n",
                    config.getSpCategoryId(), config.getChannelType());
            e.printStackTrace();
            return false; // 执行失败不计入无数据
        }
    }

    private static Connection getTargetConnection(SpCategorySql config) throws SQLException {
        String port = (config.getDbPort() == null || config.getDbPort().trim().isEmpty())
                ? "1433" : config.getDbPort();
        String url = String.format("jdbc:sqlserver://%s:%s;databaseName=%s",
                config.getDbHost(), port, config.getDbName());

        DbCredentials credentials = DB_CREDENTIALS.getOrDefault(config.getDbHost(),
                new DbCredentials(SOURCE_DB_USER, SOURCE_DB_PASSWORD));

        return DriverManager.getConnection(url, credentials.username, credentials.password);
    }

    // 数据模型类
    private static class SpCategorySql {
        private int spCategoryId;
        private int channelType;
        private String dbHost;
        private String dbPort;
        private String dbName;
        private String sqlContent;

        // Getters and Setters
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

    private static class DbCredentials {
        String username;
        String password;
        public DbCredentials(String username, String password) {
            this.username = username;
            this.password = password;
        }
    }

    private static class EmptySqlResult {
        int spCategoryId;
        int channelType;
        public EmptySqlResult(int spCategoryId, int channelType) {
            this.spCategoryId = spCategoryId;
            this.channelType = channelType;
        }
    }
}