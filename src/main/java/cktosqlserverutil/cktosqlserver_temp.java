package cktosqlserverutil;

public class cktosqlserver_temp {

/*
* 可以从clickhouse同步到sqlserver上 ，但是要注意scala类里有两行要注释掉。
* */

    public static void main(String[] args) {

//        String clickhouseTableName = "mt_new_add_0326";
//        String clickhouseDB = "ods";
//        String sqlserverconffiled ="select * from result";
//        String sqlserverurl ="jdbc:sqlserver://192.168.4.36;database=o2o";
//        String sqlserverTable ="mt_new_add_0326";
//        String sqlServerUser ="CHH";
//        String sqlServerPassword = "Y1v606";

//                String clickhouseTableName = "b2c_02";
//        String clickhouseDB = "test";
//        String sqlserverconffiled ="select * from result";
//        String sqlserverurl ="jdbc:sqlserver://192.168.4.212:2533;database=WebSearchC";
//        String sqlserverTable ="sp_b2c_0329";
//        String sqlServerUser ="CHH";
//        String sqlServerPassword = "Y1v606";


//        String clickhouseTableName = "tbtm_03";
//        String clickhouseDB = "test";
//        String sqlserverconffiled ="select * from  result;";
//        String sqlserverurl ="jdbc:sqlserver://192.168.4.201:2422;DatabaseName=Taobao_trading";
//        String sqlserverTable ="sp_tbtm_0329";
//        String sqlServerUser ="CHH";
//        String sqlServerPassword = "Y1v606";

//        String clickhouseTableName = "jd_key_quanmamalist_20241016_20241016_2_new";
//        String clickhouseDB = "ods";
//        String sqlserverconffiled ="select * from result where length(itemid) < 43";
//        String sqlserverurl ="jdbc:sqlserver://192.168.3.182:1433;DatabaseName=SyncWebSearchJD";
//        String sqlserverTable ="jd_key_quanmamalist_20241016_20241016_2_matched";
//        String sqlServerUser ="sa";
//        String sqlServerPassword = "smartpthdata";



        String clickhouseTableName = "mtoriginaldetail";
        String clickhouseDB = "ods";
        String sqlserverconffiled ="select  *  from  result where   pt_ym  >= '202301'   and pt_ym  <=   '202407'  and  IndustryId  = 73    and IndustrySubId = 148 ";
        String sqlserverurl ="jdbc:sqlserver://192.168.4.39;DatabaseName=O2O";
        String sqlserverTable ="mtoriginaldetail_wines_20251204";
        String sqlServerUser ="sa";
        String sqlServerPassword = "smartpthdata";


//        String clickhouseTableName = "trading_tm_detail";
//        String clickhouseDB = "ods";
//        String sqlserverconffiled ="select * from result";
//        String sqlserverurl ="jdbc:sqlserver://192.168.3.181:1433;DatabaseName=SyncTmallShop";
//        String sqlserverTable ="trading_tm_detail";
//        String sqlServerUser ="sa";
//        String sqlServerPassword = "smartpthdata";


        try {
            cktosql.cktosqlserver(clickhouseTableName,clickhouseDB,sqlserverconffiled,sqlserverurl,sqlserverTable, sqlServerUser, sqlServerPassword);

        } catch (Exception e) {
            throw e;
        }
    }
}
