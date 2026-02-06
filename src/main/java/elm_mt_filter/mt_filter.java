package elm_mt_filter;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/*
* 注意： 和sqlserver表名 和日期的 变量的填写
* 新加个字段poi_id_str 看看有没有出错
* */
public class mt_filter {
    private static List<O2OKeywords> keywords;
    public static void main(String[] args) {
        keywords = elm_mt_filter.getKeywords();
        List<Map<String, String>> finalList = new ArrayList<>();
        finalList.add(get_itemname_where.categorizeAndGenerateQuery(keywords));
  //      get_itemname_where.printFinalList(finalList);

        for (Map<String, String> map : finalList) {
            // 遍历当前 Map 对象中的键值对
            for (Map.Entry<String, String> entry : map.entrySet()) {
                String key = entry.getKey();
                String value = entry.getValue();

                //药店的
                datainsert(key, "202601", value,"mt_durg");
            }
        }
   //     超市的，超市不用过滤
  //      datainsert("", "202601", "1 = 1 ","mt_durg");
     }


    public static void datainsert(String industryId, String pt_ym, String whereresult,String mark) {

        // Your existing code
        // Assuming ck_util is an instance of some utility class
        ck_util.ckEXECUTE("drop table if exists test.mt_filt");
        System.out.println("删除test.mt_filt表成功");

        // Use parameters in your SQL query
        String sql = "create table test.mt_filt  ENGINE = MergeTree order by  Province AS\n" +
                "select\n" +
                "province Province,\n" +
                "city City,\n" +
                "area Area,\n" +
                "street Street,\n" +
                "shop_name ShopName,\n" +
                "shop_id ShopId,\n" +
                "categoryfirst CategoryFirst,\n" +
                "goods_id ItemId,\n" +
                "ttype,\n" +
                "category_id categoryid,\n" +
                "brandid,\n" +
                "goods_name itemname,\n" +
                "origin_price Price,\n" +
                "promotion_price PricePromotion,\n" +
                "monthqty MonthQty,\n" +
                "Price*monthqty SaleValue,\n" +
                "Price*monthqty/1000 SaleValue000,\n" +
                "PricePromotion*monthqty SaleValuePromotion,\n" +
                "PricePromotion*monthqty/1000  SaleValuePromotion000,\n" +
                "update_time UpdateTime,\n" +
                "orgtableid OrgTableId,\n" +
                "image_url Image,\n" +
                "month_saled_content monthsales,\n" +
                "search_address,\n" +
                "'" + industryId + "' industryId,\n" +
                "upccode,\n" +
                " lng shop_longitude,\n" +
                "lat  shop_latitude,\n" +
                "goods_inventory inventory,\n" +
                "status,\n" +
                "status_description,\n" +
                "goods_unit,\n" +
                "tag_id,\n" +
                "promotion_info discount,\n" +
                "shop_key,\n" +
                "adddate,\n" +
                "ser_num,\n" +
                "seriesid,\n" +
                "skuid,\n" +
                "poi_id_str  poiIdStr\n" +
                "from (select * from  ods.ods_mt where  pt_ym = '" + pt_ym + "' and mark = '" + mark +"')  where " + whereresult + " ";
        System.out.println(sql);

        ck_util.ckEXECUTE(sql);
        System.out.println(industryId + "过滤完成");

        String  clickhouseTableName = "mt_filt";
        String  clickhouseDB = "test";
        String  sqlserverconffiled = "select * from result";
        String  sqlserverurl = "jdbc:sqlserver://192.168.4.39;DatabaseName=o2o";
        String  sqlserverTable = "TradingMtDrugAllCity202601";
        String  sqlServerUser = "sa";
        String  sqlServerPassword = "smartpthdata";
        try {
            cktosqlserver.cktosqlserver(clickhouseTableName,clickhouseDB,sqlserverconffiled,sqlserverurl,sqlserverTable, sqlServerUser, sqlServerPassword);

        } catch (Exception e) {
            throw e;
        }


        // Other code for executing the SQL statement goes here
    }



}
