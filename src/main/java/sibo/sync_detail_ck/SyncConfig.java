package sibo.sync_detail_ck;

import java.sql.ResultSet;
import java.sql.SQLException;

public class SyncConfig {
    private final String channel;
    private final int industryId;
    private final String sqlServerConnection;
    private final String tableName;

    public SyncConfig(String channel, int industryId, String sqlServerConnection, String tableName) {
        this.channel = channel;
        this.industryId = industryId;
        this.sqlServerConnection = sqlServerConnection;
        this.tableName = tableName;
    }

    public String getChannel() {
        return channel;
    }

    public int getIndustryId() {
        return industryId;
    }

    public String getSqlServerConnection() {
        return sqlServerConnection;
    }

    public String getTableName() {
        return tableName;
    }

    public static SyncConfig fromResultSet(ResultSet rs) throws SQLException {
        String channel = rs.getString("Channel");
        int industryId = rs.getInt("IndustryId");
        String sqlServerConnection = rs.getString("sqlServerConnection");
        String tableName = rs.getString("tableName");
        return new SyncConfig(channel, industryId, sqlServerConnection, tableName);
    }

    @Override
    public String toString() {
        return "SyncConfig{" +
                "channel='" + channel + '\'' +
                ", industryId=" + industryId +
                ", sqlServerConnection='" + sqlServerConnection + '\'' +
                ", tableName='" + tableName + '\'' +
                '}';
    }
}