package sqlservertockutil;


/*
* 注意：其他数据库String类型含有null的话，是插不进clickhouse的string类型的，但是用nullable(String)类型大数据不好处理，含有很多字符，
* 所以这边对string 类型的做一下判断ifnull 让它如果有null的话，变成空字符 ，其他的字符都没问题
*
*
* 这是京东到家的  在4.39的trading_medicinenew 表名是ddddshoprelnewbeer
* */
public class main {

    public static void main(String[] args) {

//        String  cktable = "dwd.ceshi";
//        String sqltable ="TradingYaoPinhy1412021";

       // String sqlServerUrl = "jdbc:sqlserver://192.168.4.39;DatabaseName=trading_medicinenew";  //京东到家的啤酒维度表
         //String sqlServerUrl = "jdbc:sqlserver://192.168.4.39;DatabaseName=trading_medicine";  //elmmt的啤酒维度表
        // String sqlServerUrl = "jdbc:sqlserver://192.168.4.39;DatabaseName=o2o";
        String sqlServerUrl = "jdbc:sqlserver://192.168.4.36;DatabaseName=websearchc";
      // String sqlServerUrl = "jdbc:sqlserver://192.168.4.40;DatabaseName=websearchc";
        //String sqlServerUrl = "jdbc:sqlserver://192.168.4.41;DatabaseName=Taobao_trading";
       // String sqlServerUrl = "jdbc:sqlserver://zergzhuang6.f3322.net:2766;DatabaseName=SearchCommentYHD";
        //String sqlServerUrl = "jdbc:sqlserver://192.168.4.36;DatabaseName=o2o";
        //String sqlServerUrl = "jdbc:sqlserver://192.168.4.35;DatabaseName=o2o";
        //String sqlServerUrl = "jdbc:sqlserver://192.168.4.36;DatabaseName=o2o";
       // String sqlServerUrl = "jdbc:sqlserver://192.168.4.201:2422;DatabaseName=Taobao_trading";
       // String sqlServerUrl = "jdbc:sqlserver://192.168.4.51;DatabaseName=Taobao_trading";
        //String sqlServerUrl = "jdbc:sqlserver://192.168.4.51;DatabaseName=pricetracking";
      // String sqlServerUrl = "jdbc:sqlserver://192.168.4.57;DatabaseName=TradingDouYin";
      //String sqlServerUrl = "jdbc:sqlserver://192.168.4.99:1444;DatabaseName=trading_HM";    //葡萄酒的盒马表所在地 维度表所在地
       // String sqlServerUrl = "jdbc:sqlserver://192.168.4.57;DatabaseName=WebSearchPinduoduo";
       // String sqlServerUrl = "jdbc:sqlserver://192.168.4.201:2422;DatabaseName=WebSearchCDabaojian";
        // String sqlServerUrl = "jdbc:sqlserver://smartnew.tpddns.cn:28666;DatabaseName=NewMt";
        //String sqlServerUrl = "jdbc:sqlserver://smartnew.tpddns.cn:24222;DatabaseName=Taobao_trading";
       //String sqlServerUrl = "jdbc:sqlserver://192.168.4.212:2533;DatabaseName=WebSearchC";
       // String sqlServerUrl = "jdbc:sqlserver://192.168.4.218:2599;DatabaseName=datasystem";
       //String sqlServerUrl = "jdbc:sqlserver://smartnew.tpddns.cn:25333;DatabaseName=WebSearchC";
       // String sqlServerUrl = "jdbc:sqlserver://192.168.3.183;DatabaseName=Trading_RedBook";
       // String sqlServerUrl = "jdbc:sqlserver://192.168.3.183;DatabaseName=sourceDate";
       //String sqlServerUrl = "jdbc:sqlserver://192.168.3.182;DatabaseName=SyncWebSearchJD";

       //String sqlServerUrl = "jdbc:sqlserver://smartpath10.tpddns.cn:2988;DatabaseName=TradingKS";
      //String sqlServerUrl = "jdbc:sqlserver://smartpath10.tpddns.cn:2988;DatabaseName=TradingDouYin";
////        String sqlServerUrl = "jdbc:sqlserver://192.168.99.31:2722;DatabaseName=SearchCommentY264";
      // String sqlServerUrl = "jdbc:sqlserver://192.168.4.219;DatabaseName=datasystem";
//       String sqlServerUsername = "sa";
//       String sqlServerPassword = "smartpathdata";
        String sqlServerUsername = "CHH";
        String sqlServerPassword = "Y1v606";
        //mt_Endocrine_Respiratory_CardioCerebral_Digestive_Systems
        //elm_Endocrine_Respiratory_CardioCerebral_Digestive_Systems
//        String sqlServerUsername = "sa";
//        String sqlServerPassword = "smartpthdata";
//        String sqlServerUsername = "ldd";
//        String sqlServerPassword = "W1t459";

//        String sqltable = "ck_amplify_beidema";


        //加了条件下次用要去掉
        String cktable = "test.jd_2601" ;
        String sqltable = "jd_2601";
        //String cktable = "ods.O2O_Beer_Adjust_new_two_month";
       // String conf = mssql_ck1.getConf(cktable, sqltable, sqlServerUrl, sqlServerUsername, sqlServerPassword);
        String conf = mssql_ck2_12.getConf(cktable, sqltable, sqlServerUrl, sqlServerUsername, sqlServerPassword);



    }
}
