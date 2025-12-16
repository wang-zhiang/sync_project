package sibo.sync_detail_ck;

import java.math.BigDecimal;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;

public class SyncDataRecord {
    private final String channel;
    private final int industryId;
    private final String tableName;

    private final String itemid;
    private final String itemname;
    private final Timestamp adddate;
    private final BigDecimal price;
    private final BigDecimal qty;
    private final BigDecimal zkprice;
    private final String imagesUrl;

    public SyncDataRecord(String channel, int industryId, String tableName,
                          String itemid, String itemname, Timestamp adddate,
                          BigDecimal price, BigDecimal qty, BigDecimal zkprice, String imagesUrl) {
        this.channel = channel;
        this.industryId = industryId;
        this.tableName = tableName;
        this.itemid = itemid;
        this.itemname = itemname;
        this.adddate = adddate;
        this.price = price;
        this.qty = qty;
        this.zkprice = zkprice;
        this.imagesUrl = imagesUrl;
    }

    public static SyncDataRecord fromSqlServerRow(SyncConfig cfg, ResultSet rs) throws SQLException {
        String itemid = rs.getString("Itemid");
        String itemname = rs.getString("Itemname");
        Timestamp adddate = rs.getTimestamp("Adddate");
        BigDecimal price = rs.getBigDecimal("price");
        BigDecimal qty = rs.getBigDecimal("qty");
        BigDecimal zkprice = rs.getBigDecimal("zkprice");
        String imagesUrl = rs.getString("ImagesUrl");

        return new SyncDataRecord(
                cfg.getChannel(),
                cfg.getIndustryId(),
                cfg.getTableName(),
                itemid,
                itemname,
                adddate,
                price,
                qty,
                zkprice,
                imagesUrl
        );
    }

    public void bindToPreparedStatement(PreparedStatement ps) throws SQLException {
        // INSERT INTO test.ceshi_1110 (Channel, IndustryId, tableName, Itemid, Itemname, Adddate, price, qty, zkprice, ImagesUrl)
        ps.setString(1, channel);
        ps.setInt(2, industryId);
        ps.setString(3, tableName);
        ps.setString(4, itemid);
        ps.setString(5, itemname);
        ps.setTimestamp(6, adddate);
        ps.setBigDecimal(7, price);
        ps.setBigDecimal(8, qty);
        ps.setBigDecimal(9, zkprice);
        ps.setString(10, imagesUrl);
    }
}