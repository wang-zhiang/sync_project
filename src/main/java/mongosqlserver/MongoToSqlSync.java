package mongosqlserver;

import com.mongodb.client.*;
import com.mongodb.client.model.Projections;
import org.bson.Document;

import java.sql.*;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class MongoToSqlSync {

    private static final int BATCH_SIZE = 30000;  // 调整批量大小

    private final String sqlServerUrl;
    private final String sqlServerUser;
    private final String sqlServerPassword;
    private final String mongoDbUrl;
    private final String mongoDbName;

    private Connection sqlConnection;
    private MongoClient mongoClient;
    private MongoDatabase mongoDatabase;

    public MongoToSqlSync(String sqlServerUrl, String sqlServerUser, String sqlServerPassword,
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
            // 更新后的 SQL Server 连接 URL，增加了超时设置
            String modifiedSqlServerUrl = sqlServerUrl + ";connectTimeout=300000;socketTimeout=300000";
            sqlConnection = DriverManager.getConnection(modifiedSqlServerUrl, sqlServerUser, sqlServerPassword);
            sqlConnection.setAutoCommit(false);
            System.out.println("成功连接到 SQL Server");
        } catch (SQLException e) {
            System.err.println("无法连接到 SQL Server: " + e.getMessage());
            throw e;
        }

        try {
            // 连接 MongoDB
            mongoClient = MongoClients.create(mongoDbUrl);
            mongoDatabase = mongoClient.getDatabase(mongoDbName);
            System.out.println("成功连接到 MongoDB");
        } catch (Exception e) {
            System.err.println("无法连接到 MongoDB: " + e.getMessage());
            throw e;
        }
    }

    /**
     * 同步 MongoDB 集合到 SQL Server 表
     *
     * @param sqlServerTable  SQL Server 表名
     * @param mongoCollection MongoDB 集合名
     */
    public void mongoToSql(String sqlServerTable, String mongoCollection) {
        try {
            initializeConnections();
            MongoCollection<Document> collection = mongoDatabase.getCollection(mongoCollection);
            long totalDocuments = collection.countDocuments();
            System.out.println("开始同步 " + totalDocuments + " 条记录从 MongoDB 集合 '" + mongoCollection + "' 到 SQL Server 表 '" + sqlServerTable + "'");

            // 获取 MongoDB 中的所有字段，包括 _id
            Document sampleDoc = collection.find().projection(Projections.include()).first();
            if (sampleDoc == null) {
                System.out.println("MongoDB 集合 '" + mongoCollection + "' 是空的。");
                return;
            }

            Set<String> mongoFields = sampleDoc.keySet();

            // 检查 SQL Server 表是否存在
            if (!checkTableExists(sqlServerTable)) {
                System.out.println("SQL Server 表 '" + sqlServerTable + "' 不存在，正在创建...");
                createTable(sqlServerTable, mongoFields);
            } else {
                System.out.println("SQL Server 表 '" + sqlServerTable + "' 已存在，检查新字段...");
                updateTable(sqlServerTable, mongoFields);
            }

            // 开始同步数据
            FindIterable<Document> documents = collection.find().batchSize(BATCH_SIZE);
            MongoCursor<Document> cursor = documents.iterator();

            List<Document> batch = new ArrayList<>(BATCH_SIZE);
            int count = 0;
            while (cursor.hasNext()) {
                batch.add(cursor.next());
                if (batch.size() == BATCH_SIZE) {
                    insertBatch(sqlServerTable, batch);
                    count += batch.size();
                    System.out.println("已同步 " + count + " / " + totalDocuments + " 条记录");
                    batch.clear();
                }
            }

            // 插入剩余的数据
            if (!batch.isEmpty()) {
                insertBatch(sqlServerTable, batch);
                count += batch.size();
                System.out.println("已同步 " + count + " / " + totalDocuments + " 条记录");
            }

            sqlConnection.commit();
            System.out.println("同步完成，总共同步了 " + count + " 条记录到 '" + sqlServerTable + "'");
        } catch (Exception e) {
            System.err.println("同步过程中发生错误: " + e.getMessage());
            try {
                if (sqlConnection != null && !sqlConnection.isClosed()) {
                    sqlConnection.rollback();
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
        DatabaseMetaData dbm = sqlConnection.getMetaData();
        try (ResultSet tables = dbm.getTables(null, null, tableName, null)) {
            return tables.next();
        }
    }

    /**
     * 创建 SQL Server 表
     */
    private void createTable(String tableName, Set<String> fields) throws SQLException {
        StringBuilder createSQL = new StringBuilder("CREATE TABLE ").append(tableName).append(" (");
        List<String> columns = new ArrayList<>();
        for (String field : fields) {
            columns.add("[" + field + "] NVARCHAR(MAX)");
        }
        createSQL.append(String.join(", ", columns));
        createSQL.append(")");
        try (Statement stmt = sqlConnection.createStatement()) {
            stmt.execute(createSQL.toString());
            System.out.println("表 '" + tableName + "' 创建成功");
        } catch (SQLException e) {
            System.err.println("创建表 '" + tableName + "' 失败: " + e.getMessage());
            throw e;
        }
    }

    /**
     * 更新 SQL Server 表，添加新字段
     */
    private void updateTable(String tableName, Set<String> mongoFields) throws SQLException {
        Set<String> existingFields = getExistingFields(tableName);
        Set<String> newFields = new HashSet<>(mongoFields);
        newFields.removeAll(existingFields);

        for (String field : newFields) {
            String alterSQL = String.format("ALTER TABLE %s ADD [%s] NVARCHAR(MAX)", tableName, field);
            try (Statement stmt = sqlConnection.createStatement()) {
                stmt.execute(alterSQL);
                System.out.println("已添加新字段 '" + field + "' 到表 '" + tableName + "'");
            } catch (SQLException e) {
                System.err.println("添加字段 '" + field + "' 到表 '" + tableName + "' 失败: " + e.getMessage());
                throw e;
            }
        }

        if (newFields.isEmpty()) {
            System.out.println("表 '" + tableName + "' 中没有新的字段需要添加");
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
     * 插入一批数据到 SQL Server
     */
    private void insertBatch(String tableName, List<Document> batch) throws SQLException {
        if (batch.isEmpty()) {
            return;
        }

        // 获取所有字段，包括 _id
        Set<String> allFields = new HashSet<>();
        for (Document doc : batch) {
            allFields.addAll(doc.keySet());
        }

        List<String> columns = new ArrayList<>(allFields);
        String columnNames = String.join(", ", columns.stream().map(col -> "[" + col + "]").toArray(String[]::new));
        String placeholders = String.join(", ", java.util.Collections.nCopies(columns.size(), "?"));

        String insertSQL = String.format("INSERT INTO %s (%s) VALUES (%s)", tableName, columnNames, placeholders);

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
