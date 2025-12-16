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

// 会提前删除索引
//同步前要把ck超时的那个脚本关掉，不然会一直报错连接被取消

public class source4_ldl_month {

    private static final String CLICKHOUSE_URL = "jdbc:clickhouse://hadoop110:8123a?socket_timeout=600000&insert_timeout=600000";
    private static final String CLICKHOUSE_USER = "default";  // 替换为你的用户名
    private static final String CLICKHOUSE_PASSWORD = "smartpath";  // 替换为你的密码
    private static final String ES_HOST = "192.168.6.102";  // Elasticsearch 主机地址
    private static final int ES_PORT = 9202;  // Elasticsearch 端口
    private static final String TABLE_NAME = "source4_ldl_CategoryI";  // ClickHouse 表名

    private static RestHighLevelClient esClient;

    // 动态批次大小，默认为50000
    private static final int BATCH_SIZE = 50000;

    public static void main(String[] args) throws Exception {
        // 1. 创建 Elasticsearch 客户端并添加身份验证
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

        // 2. 创建一个 Map 来存储每个索引的 ClickHouse 数据行数
        Map<String, Integer> ckCounts = new HashMap<>();

        // 3. 创建索引与品类的映射关系
        Map<String, String> indexCategoryMap = new HashMap<>();
//        indexCategoryMap.put("source4_ldl_category_personal_care", "WHERE SP_Category_I = '个人护理' ");
//        indexCategoryMap.put("source4_ldl_category_cosmetics", "WHERE SP_Category_I = '化妆品' ");
//        indexCategoryMap.put("source4_ldl_category_furniture", "WHERE SP_Category_I = '家具' ");
//        indexCategoryMap.put("source4_ldl_category_household_appliance", "WHERE SP_Category_I = '家用电器' ");
//        indexCategoryMap.put("source4_ldl_category_home_improvement_materials", "WHERE SP_Category_I = '家装建材' ");
        indexCategoryMap.put("source4_ldl_category_mobile_phone_number", "WHERE SP_Category_I = '手机数码' ");
//        indexCategoryMap.put("source4_ldl_category_mother_and_baby", "WHERE SP_Category_I = '母婴' ");
//        indexCategoryMap.put("source4_ldl_category_computer_office", "WHERE SP_Category_I = '电脑办公' ");

        // 4. 执行每个查询条件对应的导入任务
        for (Map.Entry<String, String> entry : indexCategoryMap.entrySet()) {
            String esIndexName = entry.getKey();
            String whereCondition = entry.getValue();
            importDataToES(esIndexName, whereCondition, ckCounts);
        }

        // 5. 验证数据一致性并输出到 Excel
        validateAndExportToExcel(ckCounts);

        // 6. 关闭 Elasticsearch 客户端
        esClient.close();
    }

    // 导入数据到 Elasticsearch 的方法
    private static int importDataToES(String esIndexName, String whereCondition, Map<String, Integer> ckCounts) throws Exception {
        logInfo("开始同步数据到 Elasticsearch 索引：" + esIndexName);

        // 3.1. 删除 Elasticsearch 表中的数据
        boolean deleteSuccess = deleteDataFromES(esIndexName);
        if (deleteSuccess) {
            logSuccess("成功删除索引 " + esIndexName + " 中的数据");
        } else {
            logError("删除索引 " + esIndexName + " 中的数据失败");
        }

        // 3.2. 连接到 ClickHouse
        Connection connection = getClickHouseConnection();

        // 3.3. 使用 WHERE 条件从 ClickHouse 查询数据
        String query = "SELECT * FROM " + TABLE_NAME + " " + whereCondition;
        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery(query);

        // 获取 ClickHouse 表的行数
        List<Map<String, Object>> rowDataList = new ArrayList<>();
        int ckCount = 0;
        while (resultSet.next()) {
            Map<String, Object> rowMap = new HashMap<>();
            int columnCount = resultSet.getMetaData().getColumnCount();

            // 处理每一列
            for (int i = 1; i <= columnCount; i++) {
                String columnName = resultSet.getMetaData().getColumnName(i);
                Object value = resultSet.getObject(i);
                rowMap.put(columnName, value);
            }
            rowDataList.add(rowMap);
            ckCount++;

            // 每当达到批次大小时，执行批量插入
            if (rowDataList.size() >= BATCH_SIZE) {
                bulkInsertData(esIndexName, rowDataList);
                rowDataList.clear();  // 清空数据
            }
        }
        // 插入剩余的不足批次大小的数据
        if (!rowDataList.isEmpty()) {
            bulkInsertData(esIndexName, rowDataList);
        }

        ckCounts.put(esIndexName, ckCount);

        // 关闭资源
        resultSet.close();
        statement.close();
        connection.close();

        logSuccess("成功同步数据到 Elasticsearch 索引：" + esIndexName);

        return ckCount;  // 返回同步的 ClickHouse 行数
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
                logError("批量插入数据失败: " + bulkResponse.buildFailureMessage());
            } else {
                logSuccess("成功批量插入 " + rowDataList.size() + " 条数据");
            }
        }
    }

    // 从 Elasticsearch 删除整个索引
    private static boolean deleteDataFromES(String esIndexName) {
        try {
            // 删除整个索引
            DeleteIndexRequest deleteRequest = new DeleteIndexRequest(esIndexName);

            // 执行删除请求
            esClient.indices().delete(deleteRequest, RequestOptions.DEFAULT);

            // 强制刷新索引
            esClient.indices().refresh(new org.elasticsearch.action.admin.indices.refresh.RefreshRequest(esIndexName), RequestOptions.DEFAULT);

            logSuccess("成功删除索引 " + esIndexName + " 中的数据");
            return true;
        } catch (Exception e) {
            logError("删除索引 " + esIndexName + " 中的数据失败: " + e.getMessage());
            return false;
        }
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

    // 验证数据一致性并输出到 Excel
    private static void validateAndExportToExcel(Map<String, Integer> ckCounts) {
        // 可以实现一些验证逻辑，例如检查同步数据的一致性，或者将数据输出到 Excel 文件
        // 例如：通过 Apache POI 库实现 Excel 输出
    }

    // 日志输出
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
