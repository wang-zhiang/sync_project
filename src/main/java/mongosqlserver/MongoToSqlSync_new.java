package mongosqlserver;

import com.mongodb.client.*;
import com.mongodb.client.model.Projections;
import org.bson.Document;

import java.sql.*;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class MongoToSqlSync_new {

    private static final int BATCH_SIZE = 30000;
    private static final int SAMPLE_SIZE = 1000; // 采样文档数量

    private final String sqlServerUrl;
    private final String sqlServerUser;
    private final String sqlServerPassword;
    private final String mongoDbUrl;
    private final String mongoDbName;

    private Connection sqlConnection = null;
    private MongoClient mongoClient = null;
    private MongoDatabase mongoDatabase = null;

    public MongoToSqlSync_new(String sqlServerUrl, String sqlServerUser, String sqlServerPassword,
                          String mongoDbUrl, String mongoDbName) {
        this.sqlServerUrl = sqlServerUrl;
        this.sqlServerUser = sqlServerUser;
        this.sqlServerPassword = sqlServerPassword;
        this.mongoDbUrl = mongoDbUrl;
        this.mongoDbName = mongoDbName;
    }

    /**
     * 初始化连接
     */
    private void initializeConnections() throws SQLException {
        try {
            String modifiedSqlServerUrl = this.sqlServerUrl + ";connectTimeout=300000;socketTimeout=300000";
            this.sqlConnection = DriverManager.getConnection(modifiedSqlServerUrl, this.sqlServerUser, this.sqlServerPassword);
            this.sqlConnection.setAutoCommit(false);
            System.out.println("成功连接到 SQL Server");
        } catch (SQLException e) {
            System.err.println("无法连接到 SQL Server: " + e.getMessage());
            throw e;
        }

        try {
            this.mongoClient = MongoClients.create(this.mongoDbUrl);
            this.mongoDatabase = this.mongoClient.getDatabase(this.mongoDbName);
            System.out.println("成功连接到 MongoDB");
        } catch (Exception e) {
            System.err.println("无法连接到 MongoDB: " + e.getMessage());
            throw new SQLException("MongoDB连接失败", e);
        }
    }

    /**
     * 获取MongoDB集合中所有可能的字段名（采样前500和后500）
     */
    private Set<String> getAllFieldsFromCollection(MongoCollection<Document> collection) {
        Set<String> allFields = new HashSet<>();
        long totalDocuments = collection.countDocuments();

        System.out.println("正在分析集合字段结构...");
        System.out.println("集合总文档数: " + totalDocuments);

        // 采样前500个文档
        System.out.println("采样前500个文档...");
        FindIterable<Document> firstDocs = collection.find().limit(500);
        int firstCount = 0;
        for (Document doc : firstDocs) {
            if (doc != null) {
                allFields.addAll(doc.keySet());
            }
            firstCount++;
            if (firstCount % 100 == 0) {
                System.out.println("已分析前 " + firstCount + " 个文档，发现 " + allFields.size() + " 个字段");
            }
        }

        System.out.println("前500个文档分析完成，发现 " + allFields.size() + " 个字段");

        // 采样后500个文档（如果总数大于500）
        if (totalDocuments > 500) {
            System.out.println("采样后500个文档...");
            long skipCount = Math.max(0, totalDocuments - 500);
            FindIterable<Document> lastDocs = collection.find().skip((int)skipCount).limit(500);

            int lastCount = 0;
            for (Document doc : lastDocs) {
                if (doc != null) {
                    allFields.addAll(doc.keySet());
                }
                lastCount++;
                if (lastCount % 100 == 0) {
                    System.out.println("已分析后 " + lastCount + " 个文档，当前共发现 " + allFields.size() + " 个字段");
                }
            }
            System.out.println("后500个文档分析完成");
        } else {
            System.out.println("总文档数不足500，跳过后500采样");
        }

        System.out.println("字段分析完成，共发现 " + allFields.size() + " 个不同字段");
        System.out.println("字段列表: " + allFields);

        return allFields;
    }

    /**
     * 同步 MongoDB 集合到 SQL Server 表
     */
    public void mongoToSql(String sqlServerTable, String mongoCollection) {
        try {
            initializeConnections();

            if (this.mongoDatabase == null) {
                throw new IllegalStateException("MongoDB数据库连接未初始化");
            }

            MongoCollection<Document> collection = this.mongoDatabase.getCollection(mongoCollection);
            long totalDocuments = collection.countDocuments();
            System.out.println("开始同步 " + totalDocuments + " 条记录从 MongoDB 集合 '" + mongoCollection + "' 到 SQL Server 表 '" + sqlServerTable + "'");

            if (totalDocuments == 0) {
                System.out.println("MongoDB 集合 '" + mongoCollection + "' 是空的。");
                return;
            }

            // 获取集合中所有可能的字段
            Set<String> allMongoFields = getAllFieldsFromCollection(collection);

            // 检查 SQL Server 表是否存在
            if (!checkTableExists(sqlServerTable)) {
                System.out.println("SQL Server 表 '" + sqlServerTable + "' 不存在，正在创建...");
                createTable(sqlServerTable, allMongoFields);
            } else {
                System.out.println("SQL Server 表 '" + sqlServerTable + "' 已存在，检查新字段...");
                updateTable(sqlServerTable, allMongoFields);
            }

            // 开始同步数据
            FindIterable<Document> documents = collection.find().batchSize(BATCH_SIZE);
            MongoCursor<Document> cursor = documents.iterator();

            List<Document> batch = new ArrayList<>(BATCH_SIZE);
            int count = 0;
            while (cursor.hasNext()) {
                Document doc = cursor.next();
                if (doc != null) {
                    batch.add(doc);
                }
                if (batch.size() == BATCH_SIZE) {
                    insertBatch(sqlServerTable, batch, allMongoFields);
                    count += batch.size();
                    System.out.println("已同步 " + count + " / " + totalDocuments + " 条记录");
                    batch.clear();
                }
            }

            // 插入剩余的数据
            if (!batch.isEmpty()) {
                insertBatch(sqlServerTable, batch, allMongoFields);
                count += batch.size();
                System.out.println("已同步 " + count + " / " + totalDocuments + " 条记录");
            }

            if (this.sqlConnection != null) {
                this.sqlConnection.commit();
            }
            System.out.println("同步完成，总共同步了 " + count + " 条记录到 '" + sqlServerTable + "'");
        } catch (Exception e) {
            System.err.println("同步过程中发生错误: " + e.getMessage());
            e.printStackTrace();
            try {
                if (this.sqlConnection != null && !this.sqlConnection.isClosed()) {
                    this.sqlConnection.rollback();
                    System.out.println("事务已回滚");
                }
            } catch (SQLException ex) {
                System.err.println("回滚事务时发生错误: " + ex.getMessage());
            }
        } finally {
            closeConnections();
        }
    }

    /**
     * 检查 SQL Server 表是否存在
     */
    private boolean checkTableExists(String tableName) throws SQLException {
        if (this.sqlConnection == null) {
            throw new IllegalStateException("SQL Server连接未初始化");
        }
        DatabaseMetaData dbm = this.sqlConnection.getMetaData();
        try (ResultSet tables = dbm.getTables(null, null, tableName, null)) {
            return tables.next();
        }
    }

    /**
     * 创建 SQL Server 表
     */
    private void createTable(String tableName, Set<String> fields) throws SQLException {
        if (this.sqlConnection == null) {
            throw new IllegalStateException("SQL Server连接未初始化");
        }

        StringBuilder createSQL = new StringBuilder("CREATE TABLE [").append(tableName).append("] (");
        List<String> columns = new ArrayList<>();
        for (String field : fields) {
            columns.add("[" + field + "] NVARCHAR(MAX)");
        }
        createSQL.append(String.join(", ", columns));
        createSQL.append(")");

        try (Statement stmt = this.sqlConnection.createStatement()) {
            stmt.execute(createSQL.toString());
            System.out.println("表 '" + tableName + "' 创建成功，包含 " + fields.size() + " 个字段");
        } catch (SQLException e) {
            System.err.println("创建表 '" + tableName + "' 失败: " + e.getMessage());
            throw e;
        }
    }

    /**
     * 更新 SQL Server 表，添加新字段
     */
    private void updateTable(String tableName, Set<String> mongoFields) throws SQLException {
        if (this.sqlConnection == null) {
            throw new IllegalStateException("SQL Server连接未初始化");
        }

        Set<String> existingFields = getExistingFields(tableName);
        Set<String> newFields = new HashSet<>(mongoFields);
        newFields.removeAll(existingFields);

        for (String field : newFields) {
            String alterSQL = String.format("ALTER TABLE [%s] ADD [%s] NVARCHAR(MAX)", tableName, field);
            try (Statement stmt = this.sqlConnection.createStatement()) {
                stmt.execute(alterSQL);
                System.out.println("已添加新字段 '" + field + "' 到表 '" + tableName + "'");
            } catch (SQLException e) {
                System.err.println("添加字段 '" + field + "' 到表 '" + tableName + "' 失败: " + e.getMessage());
                throw e;
            }
        }

        if (newFields.isEmpty()) {
            System.out.println("表 '" + tableName + "' 中没有新的字段需要添加");
        } else {
            System.out.println("表 '" + tableName + "' 已添加 " + newFields.size() + " 个新字段");
        }
    }

    /**
     * 获取 SQL Server 表中已存在的字段
     */
    private Set<String> getExistingFields(String tableName) throws SQLException {
        Set<String> fields = new HashSet<>();
        DatabaseMetaData dbm = sqlConnection.getMetaData();
        try (ResultSet rs = dbm.getColumns(null, null, tableName, null)) {
            while (rs.next()) {
                String columnName = rs.getString("COLUMN_NAME");
                fields.add(columnName);
            }
        }
        return fields;
    }

    /**
     * 插入一批数据到 SQL Server（使用预定义的字段顺序）
     */
    private void insertBatch(String tableName, List<Document> batch, Set<String> allFields) throws SQLException {
        if (batch.isEmpty()) {
            return;
        }

        // 使用预定义的字段顺序
        List<String> columns = new ArrayList<>(allFields);
        String columnNames = String.join(", ", columns.stream().map(col -> "[" + col + "]").toArray(String[]::new));
        String placeholders = String.join(", ", java.util.Collections.nCopies(columns.size(), "?"));

        String insertSQL = String.format("INSERT INTO [%s] (%s) VALUES (%s)", tableName, columnNames, placeholders);

        try (PreparedStatement pstmt = sqlConnection.prepareStatement(insertSQL)) {
            for (Document doc : batch) {
                for (int i = 0; i < columns.size(); i++) {
                    String field = columns.get(i);
                    Object value = doc.get(field);
                    pstmt.setString(i + 1, value != null ? value.toString() : null);
                }
                pstmt.addBatch();
            }
            pstmt.executeBatch();
        } catch (SQLException e) {
            System.err.println("批量插入数据失败: " + e.getMessage());
            System.err.println("SQL: " + insertSQL);
            throw e;
        }
    }

    /**
     * 关闭所有连接
     */
    private void closeConnections() {
        try {
            if (sqlConnection != null && !sqlConnection.isClosed()) {
                sqlConnection.close();
                System.out.println("SQL Server 连接已关闭");
            }
        } catch (SQLException e) {
            System.err.println("关闭 SQL Server 连接时发生错误: " + e.getMessage());
        }

        if (mongoClient != null) {
            mongoClient.close();
            System.out.println("MongoDB 连接已关闭");
        }
    }
}