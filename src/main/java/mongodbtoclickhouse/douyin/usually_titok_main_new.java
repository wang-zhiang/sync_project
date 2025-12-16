package mongodbtoclickhouse.douyin;

import mongodbtoclickhouse.tiktok;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

public class usually_titok_main_new {

/*
 1.飞瓜店铺-按月(ods.feigua_shop)feigua_shop_20240802_20240802
* 2.飞瓜品牌-按月(ods.feigua_brand) feigua_brand_20240804_20240804
* 3.飞瓜数据库分类-按月(ods.feigua_dy_goods_cate_month) feigua_dy_goods_cate_month_20240803_20240803
* 4.飞瓜关键词-按月(ods.feigua_keyword_30day)feigua_keyword_30day_20240801_20240801
* 5.飞瓜关键词-按天(ods.feigua_keyword)feigua_keyword_20240813_20240812
* 6.飞瓜商品榜-按天feigua_dy_20240802_20240731
* 7.飞瓜商品榜-按月(ods.feigua_dy_month)feigua_dy_month_20240802_20240802
* 8.飞瓜本地生活-按月feigua_bdsh_month_20240802_20240701
* 9.飞瓜本地生活-按天fg_bdsh_20240813_20240812
* 10.思勃直播-按月tiktok_live_20240813_20240813
* 11.思勃店铺关键词-按天tiktok_keyword_shop_20240813_20240813
* 12.思勃关键词-按天tiktok_keyword_20240813_20240808
* 13.思勃店铺-按天tiktok_shopdata_20240813_20240730
* 14.思勃多规格-按月(ods.tiktok_sku_comment_number)tiktok_sku_comment_number_20240813_20240813
* 15.思勃达播自播-按月(ods.feigua_goods_overview_stat)feigua_goods_overview_stat_20240805_20240805    ods.feigua_goods_overview_stat
* */


    /*
    *  需要手动导的：1,2,3,4,7,8,14,15
    *  记录导的最新的表
    *  最新一次同步(20241009)
     *1.'feigua_shop_
     * 2.feigua_brand_
     * 3.feigua_dy_goods_cate_month_
     * 4.feigua_keyword_30day_
     * 7.feigua_dy_month_
     * 8.feigua_bdsh_month_
       14.tiktok_sku_comment_number_20240815_20240813
       15.feigua_goods_overview_stat_20240805_20240805
          feigua_goods_overview_stat_month_20240930_20240930  (同步到了这个表)
     *
    * */


    // 注意事项： csv文件里可以放不同类型的表，一次性同步完,  但是要记录同步到什么位置了
    public static void main(String[] args) {

        String clickhousedatabase = "ods";
        String clickhouseUrl = "jdbc:clickhouse://hadoop110:8123";
        String mongodbbase = "sc";
        String csvFile = "D:\\wzza\\develop\\idea_project\\ceshi\\src\\main\\java\\mongodbtoclickhouse\\douyin\\a.csv"; // CSV文件路径

        try (BufferedReader br = new BufferedReader(new FileReader(csvFile))) {
            String line;
            while ((line = br.readLine()) != null) {
                // 读取CSV中的每一行（MongoDB表名）
                String mongodbtable = line.trim();
                if (!mongodbtable.isEmpty()) {

                    // 根据 MongoDB 表名动态设置 server 和 clickhouseTable1
                    String server = "";
                    String clickhouseTable1 = "";

                    if (mongodbtable.contains("feigua_shop")) {
                        server = "飞瓜店铺-按月";
                        clickhouseTable1 = "feigua_shop";
                    } else if (mongodbtable.contains("feigua_brand")) {
                        server = "飞瓜品牌-按月";
                        clickhouseTable1 = "feigua_brand";
                    } else if (mongodbtable.contains("feigua_dy_goods_cate_month")) {
                        server = "飞瓜数据库分类-按月";
                        clickhouseTable1 = "feigua_dy_goods_cate_month";
                    } else if (mongodbtable.contains("feigua_keyword_30day")) {
                        server = "飞瓜关键词-按月";
                        clickhouseTable1 = "feigua_keyword_30day";
                    } else if (mongodbtable.contains("feigua_dy_month")) {
                        server = "飞瓜商品榜-按月";
                        clickhouseTable1 = "feigua_dy_month";
                    } else if (mongodbtable.contains("feigua_bdsh_month")) {
                        server = "飞瓜本地生活-按月";
                        clickhouseTable1 = "feigua_bdsh_month";
                    } else if (mongodbtable.contains("tiktok_sku_comment_number")) {
                        server = "思勃多规格-按月";
                        clickhouseTable1 = "tiktok_sku_comment_number";
                    } else if (mongodbtable.contains("feigua_goods_overview_stat")) {
                        server = "思勃达播自播-按月";
                        clickhouseTable1 = "feigua_goods_overview_stat";
                    } else {
                        // 如果没有匹配到合适的表名类型，则跳过该表
                        System.out.println("未找到匹配的表名类型，该表为: " + mongodbtable);
                        break;
                    }

                    // 拼接 clickhouseTable
                    String clickhouseTable = clickhousedatabase + "." + clickhouseTable1;

                    System.out.println("正在同步MongoDB表: " + mongodbtable + " -> ClickHouse表: " + clickhouseTable + " | 服务器: " + server);

                    // 调用同步方法
                    usually_tiktok.mongotock(clickhouseTable, clickhouseUrl, mongodbtable, mongodbbase, server);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
