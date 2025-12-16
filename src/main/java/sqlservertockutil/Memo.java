package sqlservertockutil;

import java.sql.ResultSet;
import java.sql.SQLException;

/*
* y用来输出到mysql详情表的Memo字段
* */
public class Memo {



    public static String cksyncStatus(String ckdatabase, String cktable) throws SQLException {
        //同步条数
        String synccount = "";
        String syncquery = "select count() synccount from " + ckdatabase + "." + cktable ;
        try {
            ResultSet resultSet = cksqlserverUtil.executeQuery(syncquery);

            while (resultSet.next()) {
                synccount = resultSet.getString("synccount");

            }
        } catch (SQLException e) {

            throw e;
        }
        return  synccount;
    }

    public static String sqlsyncstatus(String url, String sqlsevertable, String username, String userpassword) throws SQLException {
        //同步条数
        String synccount = "";
        String syncquery = "select count(*) synccount from " + sqlsevertable ;


        try {
            ResultSet resultSet = sqlserverUtil.executeQuery(url, username, userpassword, syncquery);

            while (resultSet.next()) {
                synccount = resultSet.getString("synccount");

            }
        } catch (SQLException e) {

            throw e;
        }
        return  synccount;
    }




    public static void main(String[] args) throws SQLException {

        //测试专用
//        //System.out.println(ckMemo("test", "AllenCSTBckDB"));
//        System.out.println(ckMemo("dws", "dws_TradingGroupHZPSkuDYColgate_w"));

//        String url = "jdbc:sqlserver://192.168.4.201:2422;DatabaseName=Taobao_trading";
//        String username = "CHH";
//        String password = "Y1v606";
//        String table  = "TradingArticlehy";
//        System.out.println(sqlMemo(url,table,username,password));

    }


}
