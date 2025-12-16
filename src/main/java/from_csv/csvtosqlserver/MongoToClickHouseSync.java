package from_csv.csvtosqlserver;

import com.mongodb.client.*;
import com.mongodb.client.model.Projections;
import org.bson.Document;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class MongoToClickHouseSync {

    // ----- ClickHouse 配置（你已提供） -----
    private static final String CLICKHOUSE_URL = "jdbc:clickhouse://hadoop110:8123";
    private static final String CLICKHOUSE_USER = "default";
    private static final String CLICKHOUSE_PASSWORD = "smartpath";

    // 批次大小（按你要求 5000）
    private static final int CK_BATCH_SIZE = 10000;

    // ----- 你需要填写的变量 -----
    private static final String CK_TABLE_NAME = "ods.mongo_key_item";     // ClickHouse 目标表名（包含 key, itemid 两列）
    private static final String KEY_VALUE = "jd_key_quanmamalist_n";        // 固定常量 key 的值

    private static final String MONGO_URI = "mongodb://smartpath:smartpthdata@192.168.5.101:27017,192.168.5.102:27017,192.168.5.103:27017"; // MongoDB 连接串
    private static final String MONGO_DATABASE = "ec";        // MongoDB 数据库名

    private static final String COLLECTION_PREFIX = "jd_key_quanmamalist_n";      // 集合前缀，例如 jdkeyword
    private static final String START_DATE = "20250428";               // 开始日期（yyyyMMdd）
    private static final String END_DATE = "20251104";                 // 结束日期（yyyyMMdd）

    private static final String MONGO_ITEM_FIELD = "itemid";           // 从文档提取的字段（可用点路径，如 meta.itemid）

    public static void main(String[] args) {
        try {
            new MongoToClickHouseSync().run();
        } catch (Exception e) {
            System.err.println("同步过程发生异常: " + e.getMessage());
            e.printStackTrace();
        }
    }

    public void run() throws Exception {
        DateTimeFormatter df = DateTimeFormatter.ofPattern("yyyyMMdd");
        LocalDate start = LocalDate.parse(START_DATE, df);
        LocalDate end = LocalDate.parse(END_DATE, df);

        try (MongoClient mongoClient = MongoClients.create(MONGO_URI);
             Connection ckConn = DriverManager.getConnection(CLICKHOUSE_URL, CLICKHOUSE_USER, CLICKHOUSE_PASSWORD);
             PreparedStatement ckStmt = ckConn.prepareStatement(
                     "INSERT INTO " + CK_TABLE_NAME + " (key, itemid) VALUES (?, ?)")
        ) {
            MongoDatabase db = mongoClient.getDatabase(MONGO_DATABASE);

            long totalInserted = 0;
            long totalSkipped = 0;

            for (LocalDate current = start; !current.isAfter(end); current = current.plusDays(1)) {
                String dateStr = current.format(df);
                String basePrefix = COLLECTION_PREFIX + "_" + dateStr;

                // 查找以 basePrefix 开头的所有集合
                List<String> matchedCollections = new ArrayList<>();
                for (String collName : db.listCollectionNames()) {
                    if (collName.startsWith(basePrefix)) {
                        matchedCollections.add(collName);
                    }
                }

                if (matchedCollections.isEmpty()) {
                    System.out.println("[" + dateStr + "] 无匹配集合：" + basePrefix);
                    continue;
                }

                for (String collName : matchedCollections) {
                    System.out.println("开始处理集合: " + collName);
                    MongoCollection<Document> coll = db.getCollection(collName);

                    // 仅投影需要的字段，减少 IO
                    FindIterable<Document> docs = coll.find()
                            .projection(Projections.include(MONGO_ITEM_FIELD))
                            .batchSize(1000);

                    int batchCount = 0;
                    long insertedThisColl = 0;
                    long skippedThisColl = 0;

                    for (Document doc : docs) {
                        Object rawValue = extractByPath(doc, MONGO_ITEM_FIELD);
                        if (rawValue == null) {
                            skippedThisColl++;
                            continue;
                        }

                        String itemidStr = toStringValue(rawValue);
                        if (itemidStr == null || itemidStr.isEmpty()) {
                            skippedThisColl++;
                            continue;
                        }

                        ckStmt.setString(1, KEY_VALUE);
                        ckStmt.setString(2, itemidStr);
                        ckStmt.addBatch();
                        batchCount++;

                        if (batchCount >= CK_BATCH_SIZE) {
                            ckStmt.executeBatch();
                            batchCount = 0;
                        }
                        insertedThisColl++;
                    }

                    if (batchCount > 0) {
                        ckStmt.executeBatch();
                    }

                    totalInserted += insertedThisColl;
                    totalSkipped += skippedThisColl;
                    System.out.printf("集合 %s 完成：插入 %d，跳过 %d%n", collName, insertedThisColl, skippedThisColl);
                }
            }

            System.out.printf("全部完成：总插入 %d，总跳过 %d%n", totalInserted, totalSkipped);
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