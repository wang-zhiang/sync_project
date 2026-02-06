package cktosqlserverutil;

public class cktosqlserver2 {

/*
* 可以从clickhouse同步到sqlserver上 ，但是要注意scala类里有两行要注释掉。
* */

    public static void main(String[] args) {

//        String clickhouseTableName = "hzp_brand_model_disanfang_processed";
//        String clickhouseDB = "ods";
//        String sqlserverconffiled ="select *  from   result";
//        String sqlserverurl ="jdbc:sqlserver://192.168.4.51;DatabaseName=Taobao_trading";
//        String sqlserverTable ="hzp_brand_model_disanfang_processed_tm";
//        String sqlServerUser ="sa";
//        String sqlServerPassword = "smartpathdata";

//        String clickhouseTableName = "BWB_traditional_channels";
//        String clickhouseTableName = "BWB_traditional_channels";
//        String clickhouseDB = "ods";
//        String sqlserverconffiled ="select * from result";
//        String sqlserverurl ="jdbc:sqlserver://192.168.4.35;DatabaseName=Taobao_trading";
//        String sqlserverTable ="BWB_traditional_channels";
//        String sqlServerUser ="CHH";
//        String sqlServerPassword = "Y1v606";



        //tmall
//        String clickhouseTableName = "shop_0129";
//        String clickhouseDB = "tmp";
//        String sqlserverconffiled ="select * from result";
//        String sqlserverurl ="jdbc:sqlserver://192.168.4.201:2422;DatabaseName=Taobao_trading";
//        String sqlserverTable ="shop_0129";
//        String sqlServerUser ="CHH";
//        String sqlServerPassword = "Y1v606";

//        String clickhouseTableName = "hzp_brand_model_disanfang_processed";
//        String clickhouseDB = "ods";
//        String sqlserverconffiled ="select *  from   result where qudao = 'B2C'";
//        String sqlserverurl ="jdbc:sqlserver://192.168.4.36;DatabaseName=WebsearchC";
//        String sqlserverTable ="hzp_brand_model_disanfang_processed_b2c";
//        String sqlServerUser ="CHH";
//        String sqlServerPassword = "Y1v606";


//         String clickhouseTableName = "ceshi_40";
//        String clickhouseDB = "tmp";
//        String sqlserverconffiled ="select * from result ";
//        String sqlserverurl ="jdbc:sqlserver://192.168.4.40;DatabaseName=WebsearchC";
//        String sqlserverTable ="item_monthly_sales_20251107";
//        String sqlServerUser ="sa";
//        String sqlServerPassword = "smartpathdata";

//        String clickhouseTableName = "ceshi_212";
//        String clickhouseDB = "tmp";
//        String sqlserverconffiled ="select * from result ";
//        String sqlserverurl ="jdbc:sqlserver://192.168.4.212:2533;DatabaseName=WebsearchC";
//        String sqlserverTable ="item_monthly_sales_20251107";
//        String sqlServerUser ="CHH";
//        String sqlServerPassword = "Y1v606";

        //b2c
//        String clickhouseTableName = "thirddata_yunwei_1748253416696971264";
//        String clickhouseDB = "dwd";
//        String sqlserverconffiled ="select * from result";
//        String sqlserverurl ="jdbc:sqlserver://192.168.4.212:2533;database=WebSearchC";
//        String sqlserverTable ="ldltbtm_twokeyword1_20240119";
//        String sqlServerUser ="CHH";
//        String sqlServerPassword = "Y1v606";

//jd
//        String clickhouseTableName = "dwd_assigned_matched";
//        String clickhouseDB = "dwd";
//        String sqlserverconffiled ="select * from result\n" +
//                "where source='SP' and channel='DOUYIN' and  pt_ym='20231001'\n" +
//                "and isdel=0 and iscommit=1;";
//        String sqlserverurl ="jdbc:sqlserver://smartpath10.tpddns.cn:2988;DatabaseName=tradingdouyin";
//        String sqlserverTable ="sp_douyin_20231001_fendao";
//        String sqlServerUser ="CHH";
//        String sqlServerPassword = "Y1v606";



//  取数 相关的

//        String clickhouseTableName = "mj";
//        String clickhouseDB = "dwd";
//        String sqlserverconffiled =" select * from  result where   pt_ym >= '202001' and pt_ym <= '202305'  and  pt_channel = 'mj'\n" +
//                " and (itemname like '%好多鱼%' or itemname like '%蘑古力%' or itemname like '%蘑菇力%')";
//        String sqlserverurl ="jdbc:sqlserver://smartnew.tpddns.cn:24222;DatabaseName=Taobao_trading";
//        String sqlserverTable ="mj_taobaotamll_20231218";
//        String sqlServerUser ="CHH";
//        String sqlServerPassword = "Y1v606";



//        String clickhouseTableName = "mj";
//        String clickhouseDB = "dwd";
//        String sqlserverconffiled ="  select * from  result where   pt_ym >= '202001' and pt_ym <= '202305'  and  pt_channel = 'jd'  and (itemname like '%好多鱼%' or itemname like '%蘑古力%' or itemname like '%蘑菇力%') ";
//        String sqlserverurl ="jdbc:sqlserver://smartnew.tpddns.cn:25333;DatabaseName=WebSearchC";
//        String sqlserverTable ="mj_jd_20231218";
//        String sqlServerUser ="CHH";
//        String sqlServerPassword = "Y1v606";

//        String clickhouseTableName = "thirddata_yunwei_1748253416696971264";
//        String clickhouseDB = "dwd";
//        String sqlserverconffiled ="select * from result";
//        String sqlserverurl ="jdbc:sqlserver://192.168.4.212:2533;database=WebSearchC";
//        String sqlserverTable ="ldltbtm_twokeyword1_20240119";
//        String sqlServerUser ="CHH";
//        String sqlServerPassword = "Y1v606";

 // 相当于4.35
//        String clickhouseTableName = "elm_wine_250303_itemname";
//        String clickhouseDB = "test";
//        String sqlserverconffiled ="select * from result";
//        String sqlserverurl ="jdbc:sqlserver://smartnew.tpddns.cn:24222;DatabaseName=o2o";
//        String sqlserverTable ="elm_wine_250303_itemname";
//        String sqlServerUser ="CHH";
//        String sqlServerPassword = "Y1v606";

        //      b2c
//
//        String clickhouseTableName = "predictions_20250910_190137";
//        String clickhouseDB = "ods";
//        String sqlserverconffiled ="select * from result";
//        String sqlserverurl ="jdbc:sqlserver://192.168.4.36;DatabaseName=websearchc";
//        String sqlserverTable ="predictions_20250910_190137";
//        String sqlServerUser ="CHH";
//        String sqlServerPassword = "Y1v606";

//        String clickhouseTableName = "pdd_imp_goods_20240427";
//        String clickhouseDB = "tmp";
//        String sqlserverconffiled ="select * from result";
//        String sqlserverurl ="jdbc:sqlserver://smartpath10.tpddns.cn:2988;DatabaseName=WebSearchPinduoduo";
//        String sqlserverTable ="pdd_imp_goods_20240427";
//        String sqlServerUser ="CHH";
//        String sqlServerPassword = "Y1v606";

//        String clickhouseTableName = "pdd_imp_goods_20240427";
//        String clickhouseDB = "tmp";
//        String sqlserverconffiled ="select * from result";
//        String sqlserverurl ="jdbc:sqlserver://smartpath10.tpddns.cn:2988;DatabaseName=WebSearchPinduoduo";
//        String sqlserverTable ="pdd_imp_goods_20240427";
//        String sqlServerUser ="CHH";
//        String sqlServerPassword = "Y1v606";

// 京东的数据

//        String clickhouseTableName = "jd_key_quanmamalist_20241016_20241016_2_new";
//        String clickhouseDB = "ods";
//        String sqlserverconffiled ="select * from result where length(itemid)>=43";
//        String sqlserverurl ="jdbc:sqlserver://192.168.3.182:1433;DatabaseName=SyncWebSearchJD";
//        String sqlserverTable ="jd_key_quanmamalist_20241016_20241016_2_notmatched";
//        String sqlServerUser ="sa";
//        String sqlServerPassword = "smartpthdata";


//        String clickhouseTableName = "kzb_dy_20260111";
//        String clickhouseDB = "ods";
//        String sqlserverconffiled ="select * from result";
//        String sqlserverurl ="jdbc:sqlserver://192.168.3.183;DatabaseName=sourcedate";
//        String sqlserverTable ="kzb_dy_20260111";
//        String sqlServerUser ="sa";
//        String sqlServerPassword = "smartpthdata";

//        String clickhouseTableName = "hzp_brand_model_disanfang_pdd_dy_processed";
//        String clickhouseDB = "ods";
//        String sqlserverconffiled ="select * from  result   ";
//        String sqlserverurl ="jdbc:sqlserver://192.168.4.57;DatabaseName=TradingDouYin";
//        String sqlserverTable ="hzp_brand_model_disanfang_pdd_dy_processed";
//        String sqlServerUser ="sa";
//        String sqlServerPassword = "smartpthdata";


//        String clickhouseTableName = "predictions_20260130_160802 ";
//        String clickhouseDB = "ods";
//        String sqlserverconffiled ="select * from  result   ";
//        String sqlserverurl ="jdbc:sqlserver://192.168.4.57;DatabaseName=WebSearchPinduoduo";
//        String sqlserverTable ="predictions_20260130_160802 ";
//        String sqlServerUser ="sa";
//        String sqlServerPassword = "smartpthdata";

        String clickhouseTableName = "mt_plastic_wrap_20260202_result";
        String clickhouseDB = "ods";
        String sqlserverconffiled ="select  *  from  result";
        String sqlserverurl ="jdbc:sqlserver://192.168.4.39;DatabaseName=O2O";
        String sqlserverTable ="mt_plastic_wrap_20260202_result";
        String sqlServerUser ="sa";
        String sqlServerPassword = "smartpthdata";

//        String clickhouseTableName = "ls_liquor_0411";
//        String clickhouseDB = "ods";
//        String sqlserverconffiled ="select * from  result  where Channel in ('淘宝','天猫') ";
//        String sqlserverurl ="jdbc:sqlserver://192.168.4.35;DatabaseName=taobao_trading";
//        String sqlserverTable ="ls_liquor_0411_tm";
//        String sqlServerUser ="CHH";
//        String sqlServerPassword = "Y1v606";


//        String clickhouseTableName = "trading_jd_detail";
//        String clickhouseDB = "ods";
//        String sqlserverconffiled ="select * from  result ";
//        String sqlserverurl ="jdbc:sqlserver://61.171.17.223:3088;DatabaseName=CommentJD";
//        String sqlserverTable ="trading_jd_detail";
//        String sqlServerUser ="sa";
//        String sqlServerPassword = "smartpthdata";


//                String clickhouseTableName = "trading_tm_detail";
////        String clickhouseDB = "ods";
////        String sqlserverconffiled ="select * from result";
////        String sqlserverurl ="jdbc:sqlserver://192.168.3.181:1433;DatabaseName=SyncTmallShop";
////        String sqlserverTable ="trading_tm_detail";
////        String sqlServerUser ="sa";
////        String sqlServerPassword = "smartpthdata";



        try {
            cktosql.cktosqlserver(clickhouseTableName,clickhouseDB,sqlserverconffiled,sqlserverurl,sqlserverTable, sqlServerUser, sqlServerPassword);

        } catch (Exception e) {
            throw e;
        }
    }
}
