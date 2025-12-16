package ck_es_new;

import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import ru.yandex.clickhouse.ClickHouseDataSource;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

public class ck_es {

    private static final String CLICKHOUSE_URL = "jdbc:clickhouse://hadoop110:8123";
    private static final String CLICKHOUSE_USER = "default";  // 替换为你的用户名
    private static final String CLICKHOUSE_PASSWORD = "smartpath";  // 替换为你的密码
    private static final String ES_HOST = "192.168.6.102";  // Elasticsearch 主机地址
    private static final int ES_PORT = 9202;  // Elasticsearch 端口
    private static final String INDEX_NAME = "source4_ldl_test";  // Elasticsearch 索引名称
    private static final String TABLE_NAME = "source4_ldl_CategoryI";  // ClickHouse 表名
    private static final String WHERE_CONDITION = "WHERE SP_Category_I = '化妆品'";  // 你的查询条件

    private static RestHighLevelClient esClient;

    public static void main(String[] args) throws Exception {
        // 1. 在代码中指定批次大小
        int batchSize = 30000;  // 默认批次大小为30000，你可以根据需要修改这个值
        System.out.println("Using batch size: " + batchSize);

        // 2. 创建 Elasticsearch 客户端并添加身份验证
        String esUsername = "elastic";  // 替换为你的ES用户名
        String esPassword = "smartpath";  // 替换为你的ES密码

        // 创建 HTTP 客户端构建器，添加身份验证
        BasicCredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(
                new AuthScope(ES_HOST, ES_PORT),
                new UsernamePasswordCredentials(esUsername, esPassword)
        );

        // 使用 HttpAsyncClientBuilder 创建 RestHighLevelClient
        esClient = new RestHighLevelClient(
                org.elasticsearch.client.RestClient.builder(
                        new org.apache.http.HttpHost(ES_HOST, ES_PORT, "http"))
                        .setHttpClientConfigCallback(httpClientBuilder ->
                                httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider)
                        )
        );

        // 3. 连接到 ClickHouse
        Connection connection = getClickHouseConnection();

        // 4. 使用 WHERE 条件从 ClickHouse 查询数据
        String query = "SELECT * FROM " + TABLE_NAME + " " + WHERE_CONDITION;

        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery(query);

        // 5. 准备 Elasticsearch 批量请求
        BulkRequest bulkRequest = new BulkRequest();

        // 6. 处理 ClickHouse 中的每一行并添加到批量请求中
        int count = 0;
        while (resultSet.next()) {
            Map<String, Object> sourceMap = new HashMap<>();
            int columnCount = resultSet.getMetaData().getColumnCount();

            // 处理每一列
            for (int i = 1; i <= columnCount; i++) {
                String columnName = resultSet.getMetaData().getColumnName(i);
                Object value = resultSet.getObject(i);
                sourceMap.put(columnName, value);
            }

            // 创建 Elasticsearch 索引请求
            bulkRequest.add(new org.elasticsearch.action.index.IndexRequest(INDEX_NAME)
                    .source(sourceMap, XContentType.JSON));

            count++;

            // 每 30000 条数据进行一次批量提交
            if (count % batchSize == 0) {
                BulkResponse bulkResponse = esClient.bulk(bulkRequest, RequestOptions.DEFAULT);
                if (bulkResponse.hasFailures()) {
                    System.out.println("Bulk insert failed: " + bulkResponse.buildFailureMessage());
                }
                bulkRequest = new BulkRequest();  // 重置批量请求
            }
        }

        // 提交剩余的数据
        if (count % batchSize != 0) {
            BulkResponse bulkResponse = esClient.bulk(bulkRequest, RequestOptions.DEFAULT);
            if (bulkResponse.hasFailures()) {
                System.out.println("Bulk insert failed: " + bulkResponse.buildFailureMessage());
            }
        }

        // 7. 关闭资源
        resultSet.close();
        statement.close();
        connection.close();
        esClient.close();
    }

    // 使用用户名和密码连接 ClickHouse
    private static Connection getClickHouseConnection() throws SQLException {
        // 将用户名和密码附加到 ClickHouse URL 中
        String url = String.format("jdbc:clickhouse://%s/%s?user=%s&password=%s",
                "hadoop110:8123",   // ClickHouse 主机和端口
                "dwd",           // 默认数据库名称
                CLICKHOUSE_USER,     // 用户名
                CLICKHOUSE_PASSWORD  // 密码
        );

        // 使用构造好的 URL 创建连接
        ClickHouseDataSource dataSource = new ClickHouseDataSource(url);
        return dataSource.getConnection();
    }
}
