package elm_mt_filter;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
/*
* 注意：mark 和sqlserver表名 和日期的 变量的填写
* */
public class elm_filter {
    private static List<O2OKeywords> keywords;
    public static void main(String[] args) {
        keywords = elm_mt_filter.getKeywords();
        List<Map<String, String>> finalList = new ArrayList<>();
        finalList.add(get_itemname_where.categorizeAndGenerateQuery(keywords));
        //get_itemname_where.printFinalList(finalList);


        // 临时创建 ，后续要删除
//        List<String> ignoreKeys = Arrays.asList(
//                "鼻腔护理", "防脱生发HF", "关节维生素", "国际肠胃药", "矿物质", "其他保健品", "祛疤产品");
//        for (Map<String, String> map : finalList) {
//            // 遍历当前 Map 对象中的键值对
//            for (Map.Entry<String, String> entry : map.entrySet()) {
//                String key = entry.getKey();
//                String value = entry.getValue();
//                if (!ignoreKeys.contains(key)){
//                    // 打印键值对
//                    System.out.println("Key: " + key + ", Value: " + value);
//                    datainsert(key, "202404", value, "eleme");
//                }
//
//            }
//        }



        //正式版
        for (Map<String, String> map : finalList) {
            // 遍历当前 Map 对象中的键值对
            for (Map.Entry<String, String> entry : map.entrySet()) {
                String key = entry.getKey();
                String value = entry.getValue();

                System.out.println("Key: " + key + ", Value: " + value);

                datainsert(key, "202511", value, "eleme");
            }
        }



    }

    public static void datainsert(String industryId, String pt_ym, String whereresult, String mark) {

        // Your existing code
        // Assuming ck_util is an instance of some utility class
        ck_util.ckEXECUTE("drop table if exists test.elm_filt");
        System.out.println("删除test.elm_filt表成功");

        // Use parameters in your SQL query
        String sql = "create table test.elm_filt  ENGINE = MergeTree order by  Province AS\n" +
                "select\n" +
                "province Province,\n" +
                "city City,\n" +
                "''ShopName,\n" +
                "shop_id ShopId,\n" +
                "categoryfirst CategoryFirst,\n" +
                "categorysecond CategorySecond,\n" +
                "goods_id ItemId,\n" +
                "goods_name itemname,\n" +
                "current_price Price,\n" +
                "original_price PriceOriginal,\n" +
                "monthqty MonthQty,\n" +
                "Price*monthqty SaleValue,\n" +
                "Price*monthqty/1000 SaleValue000,\n" +
                "inventory Inventory,\n" +
                "updatetime UpdateTime,\n" +
                "id OrgTableId,\n" +
                "upc UPC,\n" +
                "monthsales,\n" +
                "search_address,\n" +
                "ttype,\n" +
                "categoryid,\n" +
                "brandid,\n" +
                "sellerid,\n" +
                "shop,\n" +
                "img_url imgUrl,\n" +
                "'" + industryId + "' industryId,\n" + // Concatenate industryId variable
                "'' ADDDATE,\n" +
                "'' ser_num,\n" +
                "'' seriesid,\n" +
                " skuid,\n" +
                "stock_count,\n" +
                "'' sell_out,\n" +
                "discount,\n" +
                "'' sku_sort,\n" +
                "yibaoshangpin,\n" +
                "chufangyao,\n" +
                "'' yibaodianpu,\n" +
                "allsold allSold,\n" +
                "'' key_word,\n" +
                "shopname shop_name\n" +
                "from (select * from  ods.ods_elm where pt_ym = '" + pt_ym + "' and mark = '" + mark + "') where " + whereresult + " ";
        //System.out.println(sql);
//        String clickhousestr = "clickhouse-client --password smartpath --port 9090 --query=\"" +  sql + "\"";
//        System.out.print(clickhousestr);
//        ssh.executeCommand(clickhousestr);  //没有连接上ck
        ck_util.ckEXECUTE(sql);
        System.out.println(industryId + "过滤完成");

        String  clickhouseTableName = "elm_filt";
        String  clickhouseDB = "test";
        String  sqlserverconffiled = "select * from result";
        String  sqlserverurl = "jdbc:sqlserver://192.168.4.39;DatabaseName=o2o";
        String  sqlserverTable = "goods_info_eleme_week20251204";
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
