package from_csv;

import java.sql.*;
import java.util.concurrent.*;
import java.util.*;

public class usuall_all_node_table {
    // 配置参数 - 大幅增加超时时间
    private static final int CONNECTION_TIMEOUT = 1800; // 连接超时时间（秒）30分钟
    private static final int SOCKET_TIMEOUT = 3600;     // Socket超时时间（秒）60分钟
    private static final int QUERY_TIMEOUT = 3600;      // 查询执行超时时间（秒）60分钟
    private static final int MAX_RESULT_ROWS = 100;     // 最大显示结果行数
    private static final int THREAD_POOL_SIZE = 10;     // 线程池大小

    // 字符串重复方法
    private static String repeatString(char c, int count) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < count; i++) {
            sb.append(c);
        }
        return sb.toString();
    }

    public static void main(String[] args) {
        // 节点数组
        String[] nodes = {"hadoop104", "hadoop105", "hadoop106", "hadoop107", "hadoop108", "hadoop109", "hadoop110"};

        // 在这里设置要执行的SQL语句
        String sqlQuery = "insert into tmp.skuall_ceshi  select *  from dim.skuall_ready"; // 示例查询

        System.out.println("开始在所有节点上并行执行SQL: " + sqlQuery);
        System.out.println("节点列表: " + Arrays.toString(nodes));
        System.out.println("线程池大小: " + THREAD_POOL_SIZE);
        System.out.println("连接超时: " + CONNECTION_TIMEOUT + "秒, Socket超时: " + SOCKET_TIMEOUT + "秒, 查询超时: " + QUERY_TIMEOUT + "秒");

        // 创建线程池（使用自定义线程工厂命名线程）
        ExecutorService executor = Executors.newFixedThreadPool(THREAD_POOL_SIZE, new ThreadFactory() {
            private int count = 0;

            @Override
            public Thread newThread(Runnable r) {
                return new Thread(r, "ClickHouse-Executor-" + (++count));
            }
        });

        List<Future<NodeResult>> futures = new ArrayList<>();
        long totalStartTime = System.currentTimeMillis();

        // 提交任务到线程池
        for (String node : nodes) {
            Callable<NodeResult> task = () -> executeOnNode(node, sqlQuery);
            futures.add(executor.submit(task));
        }

        // 等待所有任务完成
        List<NodeResult> results = new ArrayList<>();
        for (Future<NodeResult> future : futures) {
            try {
                results.add(future.get());
            } catch (InterruptedException e) {
                System.err.println("任务被中断: " + e.getMessage());
                Thread.currentThread().interrupt();
            } catch (ExecutionException e) {
                System.err.println("任务执行异常: " + e.getCause().getMessage());
            }
        }

        // 关闭线程池
        executor.shutdown();
        long totalDuration = System.currentTimeMillis() - totalStartTime;

        // 打印汇总结果
        printSummary(results, totalDuration);
    }

    // 打印执行摘要
    private static void printSummary(List<NodeResult> results, long totalDuration) {
        System.out.println("\n" + repeatString('=', 80));
        System.out.println("所有节点执行完成! 总耗时: " + (totalDuration / 1000.0) + "秒");
        System.out.println("节点执行结果汇总:");
        System.out.println(repeatString('-', 80));

        int successCount = 0;
        int failureCount = 0;
        int totalRows = 0;

        for (NodeResult result : results) {
            if (result.success) {
                successCount++;
                System.out.printf("[%s] 执行成功! 耗时: %.2f秒", result.node, result.duration / 1000.0);

                if (result.isSelect) {
                    System.out.printf(", 返回行数: %d", result.rowCount);
                    totalRows += result.rowCount;

                    if (result.rowCount > MAX_RESULT_ROWS) {
                        System.out.printf(" (显示前%d行)", MAX_RESULT_ROWS);
                    }
                } else {
                    System.out.printf(", 影响行数: %d", result.updateCount);
                }

                System.out.println();

                if (result.resultSummary != null && !result.resultSummary.isEmpty()) {
                    System.out.println("  结果摘要: " + result.resultSummary);
                }
            } else {
                failureCount++;
                System.out.printf("[%s] 执行失败! 耗时: %.2f秒, 错误: %s\n",
                        result.node, result.duration / 1000.0, result.errorMessage);

                if (result.exceptionType != null) {
                    System.out.println("  异常类型: " + result.exceptionType);
                }

                if (result.errorDetails != null && !result.errorDetails.isEmpty()) {
                    System.out.println("  错误详情: " + result.errorDetails);
                }
            }
        }

        System.out.println(repeatString('-', 80));
        System.out.printf("成功: %d 个节点, 失败: %d 个节点\n", successCount, failureCount);

        if (totalRows > 0) {
            System.out.println("所有节点返回总行数: " + totalRows);
        }
    }

    // 节点执行结果对象
    static class NodeResult {
        String node;
        boolean success;
        boolean isSelect;
        long duration;
        int rowCount;
        int updateCount;
        String resultSummary;
        String errorMessage;
        String exceptionType;
        String errorDetails;

        NodeResult(String node) {
            this.node = node;
        }
    }

    // 在单个节点上执行SQL
    private static NodeResult executeOnNode(String node, String sqlQuery) {
        NodeResult result = new NodeResult(node);
        long startTime = System.currentTimeMillis();

        // 构建带超时参数的JDBC URL
        String url = String.format(
                "jdbc:clickhouse://%s:8123?user=default&password=smartpath" +
                        "&connect_timeout=%d" +       // 连接超时（秒）
                        "&socket_timeout=%d",         // Socket超时（秒）
                node, CONNECTION_TIMEOUT, SOCKET_TIMEOUT
        );

        System.out.printf("[%s] 正在连接节点 (%s)...\n", node, url);

        try (Connection connection = DriverManager.getConnection(url);
             Statement statement = connection.createStatement()) {

            // 设置查询超时时间（秒）
            statement.setQueryTimeout(QUERY_TIMEOUT);

            System.out.printf("[%s] 已连接，正在执行SQL: %s\n", node, sqlQuery);

            // 判断SQL类型并执行
            if (isSelectQuery(sqlQuery)) {
                result.isSelect = true;
                try (ResultSet resultSet = statement.executeQuery(sqlQuery)) {
                    result.duration = System.currentTimeMillis() - startTime;

                    // 处理结果集
                    ResultSetMetaData metaData = resultSet.getMetaData();
                    int columnCount = metaData.getColumnCount();

                    // 收集结果摘要
                    StringBuilder summary = new StringBuilder();
                    int rowCount = 0;

                    while (resultSet.next() && rowCount < MAX_RESULT_ROWS) {
                        rowCount++;
                        if (rowCount == 1) {
                            // 第一行记录详细结果
                            for (int i = 1; i <= columnCount; i++) {
                                if (i > 1) summary.append(" | ");
                                summary.append(metaData.getColumnName(i) + ": " + resultSet.getString(i));
                            }
                        }
                    }

                    result.rowCount = rowCount;
                    result.resultSummary = summary.toString();
                    result.success = true;

                    System.out.printf("[%s] 查询成功! 耗时: %.2f秒, 返回行数: %d\n",
                            node, result.duration / 1000.0, rowCount);

                    // 检查是否有更多行
                    if (resultSet.next()) {
                        result.resultSummary += " ... (更多行未显示)";
                    }
                }
            } else {
                // 执行非SELECT语句
                System.out.printf("[%s] 正在执行非查询语句...\n", node);

                boolean hasResult = statement.execute(sqlQuery);
                result.duration = System.currentTimeMillis() - startTime;

                if (hasResult) {
                    System.out.printf("[%s] 语句返回结果集\n", node);
                    try (ResultSet resultSet = statement.getResultSet()) {
                        if (resultSet.next()) {
                            result.resultSummary = resultSet.getString(1);
                        }
                    }
                } else {
                    int updateCount = statement.getUpdateCount();
                    result.updateCount = updateCount;
                    result.resultSummary = "影响行数: " + updateCount;
                }

                result.success = true;
                System.out.printf("[%s] 执行成功! 耗时: %.2f秒, %s\n",
                        node, result.duration / 1000.0, result.resultSummary);
            }

        } catch (Exception e) {
            result.duration = System.currentTimeMillis() - startTime;
            result.success = false;
            result.errorMessage = e.getMessage();
            result.exceptionType = e.getClass().getSimpleName();

            // 提取更详细的错误信息
            StringBuilder details = new StringBuilder();
            Throwable cause = e;
            while (cause != null) {
                details.append(cause.getClass().getSimpleName()).append(": ").append(cause.getMessage());
                cause = cause.getCause();
                if (cause != null) {
                    details.append(" -> ");
                }
            }
            result.errorDetails = details.toString();

            System.err.printf("[%s] 执行失败! 耗时: %.2f秒, 错误: %s\n",
                    node, result.duration / 1000.0, result.errorDetails);
        }

        return result;
    }

    // 判断是否为SELECT查询
    private static boolean isSelectQuery(String sql) {
        String upperSql = sql.trim().toUpperCase();
        return upperSql.startsWith("SELECT") ||
                upperSql.startsWith("SHOW") ||
                upperSql.startsWith("DESC") ||
                upperSql.startsWith("EXPLAIN");
    }
}