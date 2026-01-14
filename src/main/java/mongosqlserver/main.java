package mongosqlserver;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

public class main {
    public static void main(String[] args) {


//谨记csv 最后不能有空行或者会报错，但是数据时正确
        //jd 啤酒(京东到家) 的数是ec库  4.39 trading_medicinenew  sa  samrtpthdata  杨盼
        //elm mt的数据以后往4.39 o2o里导
        //京东换购是 京东榜单 4.36 webserarch  王详晶
        //elm啤酒  葡萄酒  也放在4.39  o2o
        //炼丹炉的就放4.51，taobao
        //美团全国月销 mt_mtyp  4.39
        //pdd_brand_detail_202506   导到3.182  SyncWebSearchJD
        //盒马数据 .4.99:1444;DatabaseName=ReportServer


        String mongodbbase = "ec";
        String mongourl = "mongodb://smartpath:smartpthdata@192.168.5.101:27017,192.168.5.102:27017,192.168.5.103:27017/";
        //String sqlserverurl ="jdbc:sqlserver://192.168.4.39;DatabaseName=trading_medicinenew";
         //String sqlserverurl ="jdbc:sqlserver://192.168.4.39;DatabaseName=o2o";
        //String sqlserverurl ="jdbc:sqlserver://192.168.4.36;DatabaseName=o2o";
         //String sqlserverurl ="jdbc:sqlserver://192.168.4.99:1444;DatabaseName=ReportServer";
        //String sqlserverurl ="jdbc:sqlserver://smartnew.tpddns.cn:28666;DatabaseName=NewMt";
        //String sqlserverurl ="jdbc:sqlserver://192.168.4.36;DatabaseName=WebSearchC";
       // String sqlserverurl ="jdbc:sqlserver://192.168.4.35;DatabaseName=o2o";
       // String sqlserverurl ="jdbc:sqlserver://192.168.4.51;DatabaseName=Taobao_trading";
       //  String sqlserverurl ="jdbc:sqlserver://192.168.3.181:1433;DatabaseName=SyncTmallShop";
        // String sqlserverurl ="jdbc:sqlserver://192.168.4.57;DatabaseName=TradingDouYin";
         //String sqlserverurl ="jdbc:sqlserver://192.168.4.57;DatabaseName=TradingKS";
        // String sqlserverurl ="jdbc:sqlserver://192.168.4.51;DatabaseName=Pricetracking";
        //String sqlserverurl ="jdbc:sqlserver://smartnew.tpddns.cn:28666;DatabaseName=NewMt";
       // String sqlserverurl ="jdbc:sqlserver://192.168.3.72;DatabaseName=TradingDouYin1111";
       //String sqlserverurl = "jdbc:sqlserver://smartpath10.tpddns.cn:2988;DatabaseName=TradingDouYin";
        String sqlserverurl = "jdbc:sqlserver://192.168.3.182:1433;DatabaseName=SyncWebSearchJD";  //pdd_app_mall
        //String sqlserverurl = "jdbc:sqlserver://192.168.3.183;DatabaseName=Trading_Naifen";
        //String sqlserverurl = "jdbc:sqlserver://192.168.3.183;DatabaseName=sourceDate";
       // String sqlserverurl = "jdbc:sqlserver://192.168.3.183;DatabaseName=Trading_Meizhuang";
         //String sqlserverurl = "jdbc:sqlserver://192.168.4.99:1444;DatabaseName=ReportServer";  //叮咚买菜  小象超市  盒马数据
        // String sqlserverurl = "jdbc:sqlserver://192.168.99.45:2866;DatabaseName=wph264";   //京东的经常导入
        //String sqlserverurl = "jdbc:sqlserver://61.171.17.223:3088;DatabaseName=CommentJD";   //67号机
        String sqlserverTable = "pdd_app_detail_20260114";
//        String sqlServerUser ="sa";
//        String sqlServerPassword = "smartpthdata";
                String sqlServerUser ="sa";
        String sqlServerPassword = "smartpthdata";

//        String sqlServerUser ="ldd";
//        String sqlServerPassword = "W1t459";
//        String sqlServerUser ="CHH";
//        String sqlServerPassword = "Y1v606";

        String csvFile = "D:\\wzza\\develop\\idea_project\\ceshi\\src\\main\\java\\mongosqlserver\\a.csv";

        String line = "";
        String csvSplitBy = ",";

        try (BufferedReader br = new BufferedReader(new FileReader(csvFile))) {
            //MongoToSqlSync_new mongoToSqlSync1 = new MongoToSqlSync_new(sqlserverurl, sqlServerUser, sqlServerPassword, mongourl, mongodbbase);
            MongoToSqlSyncStringSafe mongoToSqlSync1 = new MongoToSqlSyncStringSafe(sqlserverurl, sqlServerUser, sqlServerPassword, mongourl, mongodbbase);

            while ((line = br.readLine()) != null) {
                StringBuilder sb = new StringBuilder();
                String[] data = line.split(csvSplitBy);
                for (String value : data) {
                    String mongotable =value.trim();
                    System.out.println("开始同步表：" + mongotable);
                    mongoToSqlSync1.mongoToSql(sqlserverTable,mongotable);


                }

            }
        } catch (IOException e) {
            e.printStackTrace();
        }




    }
}
