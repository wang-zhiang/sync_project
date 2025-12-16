package sqlserver_util.sqlservertosqlserver;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class DbUtil {
    static {
        boolean loaded = false;
        try {
            Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
            loaded = true;
        } catch (ClassNotFoundException ignored) {}
        try {
            Class.forName("net.sourceforge.jtds.jdbc.Driver");
            loaded = true;
        } catch (ClassNotFoundException ignored) {}
        if (!loaded) {
            throw new RuntimeException("未找到 SQL Server JDBC 驱动，请在 pom.xml 添加 mssql-jdbc 或 jtds 依赖");
        }
    }

    public static Connection newSource(SyncConfig cfg) throws SQLException {
        Connection c = DriverManager.getConnection(cfg.sourceUrl, cfg.sourceUser, cfg.sourcePassword);
        c.setReadOnly(true);
        return c;
    }

    public static Connection newTarget(SyncConfig cfg) throws SQLException {
        Connection c = DriverManager.getConnection(cfg.targetUrl, cfg.targetUser, cfg.targetPassword);
        c.setAutoCommit(false);
        return c;
    }

    public static String q(String identifier) {
        return "[" + identifier.replace("]", "]]") + "]";
    }

    public static String fullName(String schema, String table) {
        return q(schema) + "." + q(table);
    }
}