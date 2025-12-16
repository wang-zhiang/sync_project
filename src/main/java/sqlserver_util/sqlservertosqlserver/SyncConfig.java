package sqlserver_util.sqlservertosqlserver;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

public class SyncConfig {
    public enum Mode { OVERWRITE, APPEND }

    public final String sourceUrl;
    public final String sourceUser;
    public final String sourcePassword;

    public final String targetUrl;
    public final String targetUser;
    public final String targetPassword;

    public final String sourceSchema;
    public final String sourceTable;
    public final String targetSchema;
    public final String targetTable;

    public final Mode mode;

    public final int pageSize;
    public final int threads;
    public final int batchSize;
    public final int fetchSize;
    public final int queryTimeoutSeconds;

    public SyncConfig(String sourceUrl, String sourceUser, String sourcePassword,
                      String targetUrl, String targetUser, String targetPassword,
                      String sourceSchema, String sourceTable,
                      String targetSchema, String targetTable,
                      Mode mode, int pageSize, int threads, int batchSize,
                      int fetchSize, int queryTimeoutSeconds) {
        this.sourceUrl = sourceUrl;
        this.sourceUser = sourceUser;
        this.sourcePassword = sourcePassword;
        this.targetUrl = targetUrl;
        this.targetUser = targetUser;
        this.targetPassword = targetPassword;
        this.sourceSchema = sourceSchema;
        this.sourceTable = sourceTable;
        this.targetSchema = targetSchema;
        this.targetTable = targetTable;
        this.mode = mode;
        this.pageSize = pageSize;
        this.threads = threads;
        this.batchSize = batchSize;
        this.fetchSize = fetchSize;
        this.queryTimeoutSeconds = queryTimeoutSeconds;
    }

    public static SyncConfig fromPropertiesFile(String path) throws IOException {
        Properties p = new Properties();
        try (FileInputStream fis = new FileInputStream(path)) {
            p.load(fis);
        }
        String sourceUrl = require(p, "source.url");
        String sourceUser = require(p, "source.user");
        String sourcePassword = require(p, "source.password");

        String targetUrl = require(p, "target.url");
        String targetUser = require(p, "target.user");
        String targetPassword = require(p, "target.password");

        String sourceSchema = p.getProperty("source.schema", "dbo");
        String sourceTable = require(p, "source.table");
        String targetSchema = p.getProperty("target.schema", "dbo");
        String targetTable = p.getProperty("target.table", sourceTable);

        String modeStr = p.getProperty("mode", "overwrite").trim().toUpperCase();
        Mode mode = "APPEND".equals(modeStr) ? Mode.APPEND : Mode.OVERWRITE;

        int pageSize = intProp(p, "page.size", 50000);
        int threads = intProp(p, "threads", Runtime.getRuntime().availableProcessors());
        int batchSize = intProp(p, "batch.size", 2000);
        int fetchSize = intProp(p, "fetch.size", 2000);
        int queryTimeoutSeconds = intProp(p, "query.timeout.seconds", 0);

        return new SyncConfig(sourceUrl, sourceUser, sourcePassword,
                targetUrl, targetUser, targetPassword,
                sourceSchema, sourceTable,
                targetSchema, targetTable,
                mode, pageSize, threads, batchSize, fetchSize, queryTimeoutSeconds);
    }

    private static String require(Properties p, String key) {
        String v = p.getProperty(key);
        if (v == null || v.trim().isEmpty()) {
            throw new IllegalArgumentException("缺少必要配置项: " + key);
        }
        return v.trim();
    }

    private static int intProp(Properties p, String key, int def) {
        String v = p.getProperty(key);
        if (v == null || v.trim().isEmpty()) return def;
        return Integer.parseInt(v.trim());
    }

    @Override
    public String toString() {
        return "SyncConfig{" +
                "sourceUrl='" + sourceUrl + '\'' +
                ", targetUrl='" + targetUrl + '\'' +
                ", source=" + sourceSchema + "." + sourceTable +
                ", target=" + targetSchema + "." + targetTable +
                ", mode=" + mode +
                ", pageSize=" + pageSize +
                ", threads=" + threads +
                ", batchSize=" + batchSize +
                ", fetchSize=" + fetchSize +
                ", queryTimeoutSeconds=" + queryTimeoutSeconds +
                '}';
    }
}