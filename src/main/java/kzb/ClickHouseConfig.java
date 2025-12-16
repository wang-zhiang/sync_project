package kzb;

public class ClickHouseConfig {
    private String[] targetNodes;
    private String user;
    private String password;

    public ClickHouseConfig(String[] targetNodes, String user, String password) {
        this.targetNodes = targetNodes;
        this.user = user;
        this.password = password;
    }


    public String getConnectionUrl(String node) {
        return String.format("jdbc:clickhouse://%s:8123?user=%s&password=%s", node, user, password);
    }

    public String[] getTargetNodes() {
        return targetNodes;
    }



}