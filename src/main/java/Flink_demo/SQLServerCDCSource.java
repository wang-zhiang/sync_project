package Flink_demo;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import java.sql.*;

public class SQLServerCDCSource extends RichSourceFunction<String> {

    private Connection connection;
    private PreparedStatement statement;
    private volatile boolean isRunning = true;
    private long lastProcessedLsn = 0; // 记录最后处理的 LSN

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        // 初始化 SQL Server 连接
        connection = DriverManager.getConnection(
                "jdbc:sqlserver://192.168.4.218:2599;databaseName=datasystem",
                "CHH", "Y1v606"
        );
        // 初始化查询语句（按 LSN 增量读取）
        String query = "SELECT * FROM cdc.dbo_skuall_ceshi_CT WHERE __$start_lsn > ?";
        statement = connection.prepareStatement(query);
    }

    @Override
    public void run(SourceContext<String> ctx) throws Exception {
        while (isRunning) {
            statement.setLong(1, lastProcessedLsn);
            ResultSet rs = statement.executeQuery();
            while (rs.next()) {
                // 解析变更数据为 JSON 格式
                String json = parseChangeToJson(rs);
                ctx.collect(json);
                // 更新 LSN
                lastProcessedLsn = rs.getLong("__$start_lsn");
            }
            Thread.sleep(5000); // 5秒轮询一次
            rs.close();
        }
    }

    private String parseChangeToJson(ResultSet rs) throws SQLException {
        // 示例：将变更数据转为 JSON 字符串
        return String.format(
                "{\"id\":%d, \"name\":\"%s\", \"operation\":%d}",
                rs.getInt("id"),
                rs.getString("name"),
                rs.getInt("__$operation")
        );
    }

    @Override
    public void cancel() {
        isRunning = false;
        try {
            if (statement != null) statement.close();
            if (connection != null) connection.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}