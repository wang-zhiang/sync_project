package sqlserver_util.sqlservertosqlserver;

public class SqlServerToSqlServerSyncApp {
    private static String resolveConfigPath(String argOrDefault) {
        java.nio.file.Path p = java.nio.file.Paths.get(argOrDefault);
        if (p.isAbsolute() && java.nio.file.Files.exists(p)) return p.toString();

        java.nio.file.Path wd = java.nio.file.Paths.get(System.getProperty("user.dir")).resolve(argOrDefault);
        if (java.nio.file.Files.exists(wd)) return wd.toString();

        try {
            java.net.URI uri = SqlServerToSqlServerSyncApp.class.getProtectionDomain().getCodeSource().getLocation().toURI();
            java.nio.file.Path codeOutDir = java.nio.file.Paths.get(uri).getParent();
            if (codeOutDir != null) {
                java.nio.file.Path co = codeOutDir.resolve(argOrDefault);
                if (java.nio.file.Files.exists(co)) return co.toString();
            }
        } catch (Exception ignored) {}

        java.nio.file.Path pkgDir = java.nio.file.Paths.get("src", "main", "java", "sqlserver_util", "sqlservertosqlserver").resolve(argOrDefault);
        if (java.nio.file.Files.exists(pkgDir)) return pkgDir.toString();

        // 回退：工作目录
        return wd.toString();
    }
    public static void main(String[] args) {
        String configName = args.length > 0 ? args[0] : "sync-config.properties";
        try {
            System.out.println("工作目录: " + System.getProperty("user.dir"));
            String cfgPath = resolveConfigPath(configName);
            System.out.println("读取配置文件: " + cfgPath);

            SyncConfig cfg = SyncConfig.fromPropertiesFile(cfgPath);
            new SqlServerTableSync().sync(cfg);
        } catch (Exception e) {
            System.err.println("执行失败: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }
}