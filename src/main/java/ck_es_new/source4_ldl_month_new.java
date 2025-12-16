package ck_es_new;

import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import ru.yandex.clickhouse.ClickHouseDataSource;

import java.sql.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

//还是不行的
public class source4_ldl_month_new {

    // 点击数据库的连接配置
    private static final String CLICKHOUSE_URL = "jdbc:clickhouse://hadoop110:8123?socket_timeout=600000&insert_timeout=600000";
    private static final String CLICKHOUSE_USER = "default";
    private static final String CLICKHOUSE_PASSWORD = "smartpath";
    private static final String ES_HOST = "192.168.6.102";
    private static final int ES_PORT = 9202;
    private static final String TABLE_NAME = "dwd.source4_ldl_CategoryI";

    // 批次大小
    private static final int BATCH_SIZE = 30000;
    // 线程池大小
    private static final int THREAD_POOL_SIZE = 5;

    // 存储总记录数的计数器
    private static final AtomicInteger offsetCounter = new AtomicInteger(0);

    private static RestHighLevelClient esClient;
    private static ExecutorService executorService;

    public static void main(String[] args) throws Exception {
        // 1. 创建 Elasticsearch 客户端并添加身份验证
        String esUsername = "elastic";
        String esPassword = "smartpath";

        BasicCredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(
                new AuthScope(ES_HOST, ES_PORT),
                new UsernamePasswordCredentials(esUsername, esPassword)
        );

        esClient = new RestHighLevelClient(
                org.elasticsearch.client.RestClient.builder(
                        new org.apache.http.HttpHost(ES_HOST, ES_PORT, "http"))
                        .setHttpClientConfigCallback(httpClientBuilder ->
                                httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider)
                        )
        );

        // 2. 初始化线程池
        executorService = Executors.newFixedThreadPool(THREAD_POOL_SIZE);

        // 3. 创建索引与品类的映射关系
        Map<String, String> indexCategoryMap = new HashMap<>();
        indexCategoryMap.put("source4_ldl_category_test", "WHERE SP_Category_I = '手机数码' ");

        // 4. 使用多线程同步数据到 Elasticsearch
        List<Callable<Void>> tasks = new ArrayList<>();
        for (Map.Entry<String, String> entry : indexCategoryMap.entrySet()) {
            String esIndexName = entry.getKey();
            String whereCondition = entry.getValue();
            tasks.add(() -> importDataToES(esIndexName, whereCondition));
        }

        // 5. 执行任务
        List<Future<Void>> futures = executorService.invokeAll(tasks);

        // 6. 等待所有线程执行完毕
        for (Future<Void> future : futures) {
            try {
                future.get();
            } catch (InterruptedException | ExecutionException e) {
                logError("同步数据时发生错误: " + e.getMessage());
            }
        }

        // 7. 关闭 Elasticsearch 客户端和线程池
        esClient.close();
        executorService.shutdown();
    }

    private static Void importDataToES(String esIndexName, String whereCondition) throws Exception {
        logInfo("线程 " + Thread.currentThread().getName() + " 开始同步数据到 Elasticsearch 索引：" + esIndexName);

        // 删除 Elasticsearch 索引中的数据
        boolean deleteSuccess = deleteDataFromES(esIndexName);
        if (deleteSuccess) {
            logSuccess("线程 " + Thread.currentThread().getName() + " 成功删除索引 " + esIndexName + " 中的数据");
        } else {
            logError("线程 " + Thread.currentThread().getName() + " 删除索引 " + esIndexName + " 中的数据失败");
        }

        // 连接到 ClickHouse
        Connection connection = getClickHouseConnection();

        // 获取数据总行数
        int totalCount = getTotalCountFromClickHouse(connection, whereCondition);
        int partitions = 5;  // 分成5个线程进行处理
        int recordsPerPartition = totalCount / partitions;

        List<Callable<Void>> tasks = new ArrayList<>();
        for (int i = 0; i < partitions; i++) {
            // 将每个分片的查询任务提交给线程池
            final int partition = i;
            tasks.add(() -> processPartitionAndInsertToES(esIndexName, whereCondition, partition, recordsPerPartition, connection));
        }

        // 执行线程任务
        List<Future<Void>> futures = executorService.invokeAll(tasks);

        // 等待所有线程完成
        for (Future<Void> future : futures) {
            future.get();
        }

        logSuccess("线程 " + Thread.currentThread().getName() + " 成功同步数据到 Elasticsearch 索引：" + esIndexName);

        // 验证数据同步完整性
        validateDataInES(esIndexName, totalCount);

        // 关闭资源
        connection.close();

        return null;
    }

    private static Void processPartitionAndInsertToES(String esIndexName, String whereCondition, int partition, int recordsPerPartition, Connection connection) throws Exception {
        String query = "SELECT * FROM " + TABLE_NAME + " " + whereCondition +
                " LIMIT " + recordsPerPartition + " OFFSET " + (partition * recordsPerPartition);

        logInfo("线程 " + Thread.currentThread().getName() + " 执行查询: " + query);

        Statement stmt = connection.createStatement();
        ResultSet resultSet = stmt.executeQuery(query);

        List<Map<String, Object>> rowDataList = new ArrayList<>();
        while (resultSet.next()) {
            Map<String, Object> rowMap = new HashMap<>();
            int columnCount = resultSet.getMetaData().getColumnCount();
            for (int i = 1; i <= columnCount; i++) {
                String columnName = resultSet.getMetaData().getColumnName(i);
                Object value = resultSet.getObject(i);
                rowMap.put(columnName, value);
            }
            rowDataList.add(rowMap);

            // 每当达到批次大小时，执行批量插入
            if (rowDataList.size() >= BATCH_SIZE) {
                bulkInsertData(esIndexName, rowDataList);
                logInfo("线程 " + Thread.currentThread().getName() + " 已批量插入 " + rowDataList.size() + " 条数据");
                rowDataList.clear();
            }
        }

        if (!rowDataList.isEmpty()) {
            bulkInsertData(esIndexName, rowDataList);
            logInfo("线程 " + Thread.currentThread().getName() + " 最后批量插入 " + rowDataList.size() + " 条数据");
        }

        resultSet.close();
        stmt.close();

        return null;
    }

    // 验证数据是否成功同步到 Elasticsearch
    private static void validateDataInES(String esIndexName, int expectedCount) throws Exception {
        long countInEs = getEsDocumentCount(esIndexName);
        if (countInEs != expectedCount) {
            logError("数据插入不完整，索引 " + esIndexName + " 中的数据条数为 " + countInEs + "，但预期应为 " + expectedCount);
        } else {
            logSuccess("验证成功，索引 " + esIndexName + " 中的文档数量为 " + countInEs);
        }
    }

    // 获取 Elasticsearch 索引中的文档数
    private static long getEsDocumentCount(String esIndexName) throws Exception {
        org.elasticsearch.action.search.SearchRequest searchRequest = new org.elasticsearch.action.search.SearchRequest(esIndexName);
        searchRequest.source().query(org.elasticsearch.index.query.QueryBuilders.matchAllQuery()).size(0);
        org.elasticsearch.action.search.SearchResponse searchResponse = esClient.search(searchRequest, RequestOptions.DEFAULT);
        return searchResponse.getHits().getTotalHits().value;
    }

    // 批量插入数据到 Elasticsearch
    private static void bulkInsertData(String esIndexName, List<Map<String, Object>> rowDataList) throws Exception {
        BulkRequest bulkRequest = new BulkRequest();
        for (Map<String, Object> rowMap : rowDataList) {
            bulkRequest.add(new org.elasticsearch.action.index.IndexRequest(esIndexName)
                    .source(rowMap, XContentType.JSON));
        }

        // 批量提交
        if (!rowDataList.isEmpty()) {
            BulkResponse bulkResponse = esClient.bulk(bulkRequest, RequestOptions.DEFAULT);
            if (bulkResponse.hasFailures()) {
                logError("批量插入 Elasticsearch 数据失败: " + bulkResponse.buildFailureMessage());
            } else {
                logSuccess("批量插入 Elasticsearch 数据成功");
            }
        }
    }

    // 获取 ClickHouse 连接
    private static Connection getClickHouseConnection() throws SQLException {
        ClickHouseDataSource dataSource = new ClickHouseDataSource(CLICKHOUSE_URL);
        return dataSource.getConnection(CLICKHOUSE_USER, CLICKHOUSE_PASSWORD);
    }

    // 获取 ClickHouse 中符合条件的总记录数
    private static int getTotalCountFromClickHouse(Connection connection, String whereCondition) throws SQLException {
        String countQuery = "SELECT COUNT(*) FROM " + TABLE_NAME + " " + whereCondition;
        Statement stmt = connection.createStatement();
        ResultSet resultSet = stmt.executeQuery(countQuery);
        resultSet.next();
        return resultSet.getInt(1);
    }

    // 删除 Elasticsearch 中的旧数据
    private static boolean deleteDataFromES(String esIndexName) {
        try {
            DeleteIndexRequest deleteRequest = new DeleteIndexRequest(esIndexName);
            esClient.indices().delete(deleteRequest, RequestOptions.DEFAULT);
            esClient.indices().refresh(new org.elasticsearch.action.admin.indices.refresh.RefreshRequest(esIndexName), RequestOptions.DEFAULT);
            return true;
        } catch (Exception e) {
            logError("删除 Elasticsearch 中数据失败: " + e.getMessage());
            return false;
        }
    }

    // 日志方法
    private static void logInfo(String message) {
        System.out.println("[INFO] " + message);
    }

    private static void logSuccess(String message) {
        System.out.println("[SUCCESS] " + message);
    }

    private static void logError(String message) {
        System.err.println("[ERROR] " + message);
    }
}
