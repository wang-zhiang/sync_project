package mongodbtoclickhouse.douyin;

import mongodbtoclickhouse.tiktok;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

public class usually_titok_main {
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
* 新增：
*   按天：
*   jdprice
*   tm_sku_price_20240904_240901wl  (带wl的都要同步)
*    还有新增的
* */


/*
*  需要手动导的：1,2,3,4,7,8,14,15
*  记录导的最新的表
*  最新一次同步20230910
 *1.'feigua_shop_20240902_20240902'   240910
 * 2.feigua_brand_20240902_20240902  240910
 * 3.feigua_dy_goods_cate_month_20240903_20240903    240910
 * 4.feigua_keyword_30day_20240903_20240903     240910
 * 7.feigua_dy_month_20240902_20240902       240910
 * 8.feigua_bdsh_month_20240902_20240801        240910
   14.tiktok_sku_comment_number_20240815_20240813    240910
   15.feigua_goods_overview_stat_20240805_20240805
      feigua_goods_overview_stat_month_20240910_20240909     240910
 *
* */

    public static void main(String[] args) {

        String clickhousedatabase = "ods";
        String clickhouseTable1 = "feigua_shop"; // 这里不能加库名，加库名会报错
        String server = "飞瓜店铺-按月";
        String clickhouseUrl = "jdbc:clickhouse://hadoop110:8123";
        String mongodbbase = "sc";
        String clickhouseTable = clickhousedatabase + "." + clickhouseTable1;

        String csvFile = "D:\\wzza\\develop\\idea_project\\ceshi\\src\\main\\java\\mongodbtoclickhouse\\douyin\\a.csv"; // CSV文件路径

        try (BufferedReader br = new BufferedReader(new FileReader(csvFile))) {
            String line;
            while ((line = br.readLine()) != null) {
                // 读取CSV中的每一行（MongoDB表名）
                String mongodbtable = line.trim();
                if (!mongodbtable.isEmpty()) {
                    System.out.println("正在同步MongoDB表: " + mongodbtable);
                    // 调用同步方法
                    usually_tiktok.mongotock(clickhouseTable, clickhouseUrl, mongodbtable, mongodbbase, server);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
