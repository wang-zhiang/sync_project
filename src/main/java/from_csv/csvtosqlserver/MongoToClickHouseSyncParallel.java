package from_csv.csvtosqlserver;

import com.mongodb.client.*;
import com.mongodb.client.model.Projections;
import org.bson.Document;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

public class MongoToClickHouseSyncParallel {

    // ----- ClickHouse 配置（与 MongoToClickHouseSync 保持一致） -----
    private static final String CLICKHOUSE_URL = "jdbc:clickhouse://hadoop110:8123";
    private static final String CLICKHOUSE_USER = "default";
    private static final String CLICKHOUSE_PASSWORD = "smartpath";

    // 批次大小（保留你要求的 5000）
    private static final int CK_BATCH_SIZE = 5000;

    // 并发与队列参数（可按机器能力调节）
    private static final int WRITER_THREADS = Math.max(2, Runtime.getRuntime().availableProcessors()); // 写入线程数
    private static final int PRODUCER_THREADS = Math.max(2, Runtime.getRuntime().availableProcessors()); // 读取线程池大小
    private static final int QUEUE_CAPACITY = 50_000; // 队列容量，避免过大占内存

    // ----- 目标配置信息（参考 MongoToClickHouseSync） -----
    private static final String CK_TABLE_NAME = "ods.mongo_key_item";     // ClickHouse 目标表名（列：key, itemid）
    private static final String KEY_VALUE = "jdprice";      // 固定常量 key 的值

    private static final String MONGO_URI = "mongodb://smartpath:smartpthdata@192.168.5.101:27017,192.168.5.102:27017,192.168.5.103:27017"; // MongoDB 连接串
    private static final String MONGO_DATABASE = "ec";                    // MongoDB 数据库名

    private static final String COLLECTION_PREFIX = "jdprice"; // 集合前缀
    private static final String START_DATE = "20250101";                     // 开始日期（yyyyMMdd）
    //private static final String END_DATE = "20240106";                       // 结束日期（yyyyMMdd）
    private static final String END_DATE = "20251104";                       // 结束日期（yyyyMMdd）

    private static final String MONGO_ITEM_FIELD = "itemid";                 // 从文档提取的字段（可用点路径，如 meta.itemid）

    // 队列中传递的记录类型
    private static class Row {
        final String itemid;
        final boolean poison; // 终止标记

        Row(String itemid) { this.itemid = itemid; this.poison = false; }
        Row(boolean poison) { this.itemid = null; this.poison = poison; }
    }

    public static void main(String[] args) {
        try {
            new MongoToClickHouseSyncParallel().run();
        } catch (Exception e) {
            System.err.println("同步过程发生异常: " + e.getMessage());
            e.printStackTrace();
        }
    }

    public void run() throws Exception {
        DateTimeFormatter df = DateTimeFormatter.ofPattern("yyyyMMdd");
        LocalDate start = LocalDate.parse(START_DATE, df);
        LocalDate end = LocalDate.parse(END_DATE, df);

        BlockingQueue<Row> queue = new ArrayBlockingQueue<>(QUEUE_CAPACITY);
        AtomicLong totalQueued = new AtomicLong(0);
        AtomicLong totalInserted = new AtomicLong(0);
        AtomicLong totalSkipped = new AtomicLong(0);

        // 新增：轻量队列监控日志（每5秒打印一次队列占用）
        ScheduledExecutorService monitor = Executors.newSingleThreadScheduledExecutor();
        monitor.scheduleAtFixedRate(() -> {
            int size = queue.size();
            double pct = (size * 100.0) / QUEUE_CAPACITY;
            System.out.printf("[Queue] size=%d/%d (%.1f%%)%n", size, QUEUE_CAPACITY, pct);
            if (pct >= 80.0) {
                System.out.println("[Queue] 警告：队列占用≥80%，可考虑增大 WRITER_THREADS 或 QUEUE_CAPACITY");
            }
        }, 0, 5, TimeUnit.SECONDS);

        // 单个 MongoClient，线程安全
        try (MongoClient mongoClient = MongoClients.create(MONGO_URI)) {
            MongoDatabase db = mongoClient.getDatabase(MONGO_DATABASE);

            // 预先枚举所有集合名，过滤出目标日期范围的集合（避免每天都枚举）
            List<String> allNames = new ArrayList<>();
            for (String name : db.listCollectionNames()) {
                allNames.add(name);
            }

            List<String> targetCollections = filterCollections(allNames, COLLECTION_PREFIX, start, end, df);
            if (targetCollections.isEmpty()) {
                System.out.println("没有匹配到任何集合，前缀：" + COLLECTION_PREFIX);
                return;
            }
            System.out.println("匹配到集合数量：" + targetCollections.size());

            // 启动写入线程池
            ExecutorService consumerPool = Executors.newFixedThreadPool(WRITER_THREADS);
            List<Future<?>> consumerFutures = new ArrayList<>();
            for (int i = 0; i < WRITER_THREADS; i++) {
                consumerFutures.add(consumerPool.submit(() -> consumeAndInsert(queue, totalInserted)));
            }

            // 启动读取线程池，按集合并行读取
            ExecutorService producerPool = Executors.newFixedThreadPool(PRODUCER_THREADS);
            List<Future<?>> producerFutures = new ArrayList<>();
            for (String collName : targetCollections) {
                producerFutures.add(producerPool.submit(() ->
                        produceFromCollection(db.getCollection(collName), queue, totalQueued, totalSkipped)));
            }

            // 等待读取全部结束
            producerPool.shutdown();
            producerPool.awaitTermination(7, TimeUnit.DAYS);

            // 向每个消费者发送 poison pill
            for (int i = 0; i < WRITER_THREADS; i++) {
                queue.put(new Row(true));
            }

            // 等待写入线程结束
            consumerPool.shutdown();
            consumerPool.awaitTermination(7, TimeUnit.DAYS);

            // 检查写入线程可能抛出的异常
            for (Future<?> f : consumerFutures) {
                try {
                    f.get();
                } catch (ExecutionException ee) {
                    throw new RuntimeException("写入线程异常: " + ee.getCause().getMessage(), ee.getCause());
                }
            }

            // 新增：停止监控线程
            monitor.shutdownNow();

            System.out.printf("全部完成：排队 %d，插入 %d，跳过 %d%n",
                    totalQueued.get(), totalInserted.get(), totalSkipped.get());
        }
    }

    private List<String> filterCollections(List<String> allNames, String prefix,
                                           LocalDate start, LocalDate end, DateTimeFormatter df) {
        // 仅保留以 prefix_yyyyMMdd 开头的集合，同时 yyyyMMdd 在 [start, end] 区间
        List<String> matched = new ArrayList<>();
        for (String name : allNames) {
            if (!name.startsWith(prefix + "_")) continue;

            // 修复点：用前缀长度 + 1 来定位日期起始位置，而不是 name.indexOf('_') + 1
            int idx = prefix.length() + 1;
            if (idx + 8 > name.length()) continue;

            String datePart = name.substring(idx, idx + 8);
            try {
                LocalDate d = LocalDate.parse(datePart, df);
                if (!d.isBefore(start) && !d.isAfter(end)) {
                    matched.add(name); // 支持后续再跟下划线和任意后缀，例如 tm_chaoshi_new_20251031_251027
                }
            } catch (Exception ignore) {
                // 日期不是 yyyyMMdd 格式则忽略
            }
        }
        return matched;
        // 如果你需要处理同一天的多个后缀集合（如额外后缀），本逻辑已包含。
    }

    private void produceFromCollection(MongoCollection<Document> coll,
                                       BlockingQueue<Row> queue,
                                       AtomicLong totalQueued,
                                       AtomicLong totalSkipped) {
        String collName = coll.getNamespace().getCollectionName();
        System.out.println("开始读取集合：" + collName);

        try {
            FindIterable<Document> docs = coll.find()
                    .projection(Projections.include(MONGO_ITEM_FIELD))
                    .batchSize(2000);

            long readCount = 0;
            long skipped = 0;

            for (Document doc : docs) {
                readCount++;
                Object rawValue = extractByPath(doc, MONGO_ITEM_FIELD);
                if (rawValue == null) {
                    skipped++;
                    continue;
                }
                String itemidStr = toStringValue(rawValue);
                if (itemidStr == null || itemidStr.isEmpty()) {
                    skipped++;
                    continue;
                }

                // 入队（阻塞，受队列容量限制，形成背压）
                queue.put(new Row(itemidStr));
                totalQueued.incrementAndGet();
            }

            totalSkipped.addAndGet(skipped);
            System.out.printf("集合 %s 读取完成：读取 %d，跳过 %d%n", collName, readCount, skipped);
        } catch (Exception e) {
            System.err.println("读取集合 " + collName + " 发生异常：" + e.getMessage());
            e.printStackTrace();
        }
    }

    private void consumeAndInsert(BlockingQueue<Row> queue, AtomicLong totalInserted) {
        // 每个写入线程单独使用一个 ClickHouse 连接与 PreparedStatement
        try (Connection ckConn = DriverManager.getConnection(CLICKHOUSE_URL, CLICKHOUSE_USER, CLICKHOUSE_PASSWORD);
             PreparedStatement ckStmt = ckConn.prepareStatement(
                     "INSERT INTO " + CK_TABLE_NAME + " (key, itemid) VALUES (?, ?)")) {

            int batchCount = 0;
            long inserted = 0;

            while (true) {
                Row row = queue.take();
                if (row.poison) {
                    // 刷新剩余批次
                    if (batchCount > 0) {
                        ckStmt.executeBatch();
                        batchCount = 0;
                    }
                    totalInserted.addAndGet(inserted);
                    break;
                }

                ckStmt.setString(1, KEY_VALUE);
                ckStmt.setString(2, row.itemid);
                ckStmt.addBatch();
                batchCount++;
                inserted++;

                if (batchCount >= CK_BATCH_SIZE) {
                    ckStmt.executeBatch();
                    batchCount = 0;
                }
            }
        } catch (Exception e) {
            throw new RuntimeException("写入线程异常: " + e.getMessage(), e);
        }
    }

    // 支持点路径取值（如 "meta.itemid"）
    private static Object extractByPath(Document doc, String path) {
        String[] parts = path.split("\\.");
        Object cur = doc;
        for (String p : parts) {
            if (cur instanceof Document) {
                cur = ((Document) cur).get(p);
            } else if (cur instanceof Map<?, ?>) {
                cur = ((Map<?, ?>) cur).get(p);
            } else {
                return null;
            }
            if (cur == null) return null;
        }
        return cur;
    }

    // 将各种类型转换为字符串；文档转 JSON，其他用 toString
    private static String toStringValue(Object v) {
        if (v == null) return null;
        if (v instanceof String) {
            return ((String) v).trim();
        } else if (v instanceof Number || v instanceof Boolean) {
            return String.valueOf(v);
        } else if (v instanceof Document) {
            return ((Document) v).toJson();
        } else if (v instanceof List<?>) {
            return String.valueOf(v);
        } else {
            return String.valueOf(v);
        }
    }
}