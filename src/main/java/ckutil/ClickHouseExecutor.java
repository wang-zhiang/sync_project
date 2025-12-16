package ckutil;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class ClickHouseExecutor {
    public static void main(String[] args) {
        // JDBC URL,用户名和密码
        String url = "jdbc:clickhouse://hadoop110:8123";
        String user = "default";
        String password = "smartpath";

        // 定义多个 pt_ym 值
        List<String> ptYmValues = new ArrayList<>();
        ptYmValues.add("202308");
        ptYmValues.add("202309");
        ptYmValues.add("202310");
        ptYmValues.add("202311");
        ptYmValues.add("202312");
        ptYmValues.add("202401");
        ptYmValues.add("202402");
        ptYmValues.add("202403");
        ptYmValues.add("202404");
        ptYmValues.add("202405");
        ptYmValues.add("202406");

        // SQL语句，使用 ? 占位符来传入 pt_ym 参数
        String insertSQL = "INSERT INTO test.ldl_0813_new\n" +
                "SELECT\n" +
                "    LEFT(a.pt_ym, 4) AS Year,\n" +
                "    a.pt_ym AS YM,\n" +
                "    CONCAT(LEFT(a.pt_ym, 4), '0', TO_STRING(QUARTER(TO_DATE_TIME(TO_DATE(CONCAT(pt_ym, '01')))))) AS YQ,\n" +
                "    '' AS SP_Category_I,\n" +
                "    '' AS SP_Category_II,\n" +
                "    '' AS SP_Category_III,\n" +
                "    '' AS SP_Brand_CNEN,\n" +
                "    '' AS SP_Brand_CN,\n" +
                "    '' AS SP_Brand_EN,\n" +
                "    CASE\n" +
                "        WHEN a.pt_channel = 'tmall' THEN '天猫'\n" +
                "        WHEN a.pt_channel = 'taobao' THEN '淘宝'\n" +
                "        ELSE ''\n" +
                "    END AS Channel,\n" +
                "    '' AS SP_Shoptype,\n" +
                "    a.shopname AS Shop,\n" +
                "    a.shopurl AS Shopurl,\n" +
                "    a.starts AS Starts,\n" +
                "    a.title AS Itemname,\n" +
                "    a.goods_id AS Itemid,\n" +
                "    a.goodurl AS URL,\n" +
                "    a.images AS Images,\n" +
                "    TO_FLOAT64_OR_ZERO(a.salesamount) AS Value_RMB,\n" +
                "    TO_FLOAT64_OR_ZERO(a.salesamount) / 1000 AS Value_RMB_000,\n" +
                "    TO_FLOAT64_OR_ZERO(a.sellcount) AS Volume_Pack,\n" +
                "    TO_FLOAT64_OR_ZERO(a.sellcount) / 1000 AS Volume_Pack_000,\n" +
                "    TO_FLOAT64_OR_ZERO(a.pricetext) AS Price,\n" +
                "    TO_FLOAT64_OR_ZERO(a.original_cost) AS Original_Price,\n" +
                "    '' AS Soldquantity,\n" +
                "    IF(b.itemid = '', 'source4', 'LDL') AS Ser_Num,\n" +
                "    a.province AS Province,\n" +
                "    a.city AS City,\n" +
                "    a.district AS District,\n" +
                "    '' AS Function1,\n" +
                "    '' AS Function2,\n" +
                "    '' AS Function3,\n" +
                "    '' AS Function4,\n" +
                "    '' AS Function5,\n" +
                "    a.shoptype AS S4_shoptype,\n" +
                "    a.title AS S4_title,\n" +
                "    a.original_cost AS S4_original_cost,\n" +
                "    a.shopurl AS S4_shopurl,\n" +
                "    a.images AS S4_images,\n" +
                "    a.platformname AS S4_platform,\n" +
                "    a.starts AS S4_starts,\n" +
                "    a.brandname AS S4_brandname,\n" +
                "    a.city AS S4_city,\n" +
                "    a.district AS S4_district,\n" +
                "    a.timeformatmonth AS S4_timeformatmonth,\n" +
                "    a.pricetext AS S4_pricetext,\n" +
                "    a.shopname AS S4_shopname,\n" +
                "    a.shopid AS S4_shopid,\n" +
                "    a.goodurl AS S4_goodurl,\n" +
                "    a.goods_id AS S4_goods_id,\n" +
                "    a.salesamount AS S4_salesamount,\n" +
                "    a.province AS S4_province,\n" +
                "    a.sellcount AS S4_sellcount,\n" +
                "    a.rootcategoryid AS S4_rootcategoryid,\n" +
                "    a.rootcategoryname AS S4_rootcategoryname,\n" +
                "    a.categoryid AS S4_categoryid,\n" +
                "    a.categoryname AS S4_categoryname,\n" +
                "    a.subcategoryid AS S4_subcategoryid,\n" +
                "    a.subcategoryname AS S4_subcategoryname,\n" +
                "    a.detailcategoryid AS S4_detailcategoryid,\n" +
                "    a.detailcategoryname AS S4_detailcategoryname,\n" +
                "    a.soldquantity AS S4_soldquantity,\n" +
                "    a.crawl_time AS S4_crawl_time,\n" +
                "    b.adddate AS LDL_adddate,\n" +
                "    b.itemname AS LDL_itemname,\n" +
                "    b.itemid AS LDL_itemid,\n" +
                "    b.brand2 AS LDL_brand,\n" +
                "    b.shopname AS LDL_shopname2,\n" +
                "    b.categoryname1 AS LDL_categoryname1,\n" +
                "    '' AS LDL_categorychg,\n" +
                "    b.categoryname2 AS LDL_categoryname2,\n" +
                "    '' AS LDL_shopname,\n" +
                "    '' AS LDL_credit,\n" +
                "    '' AS LDL_marketing_promotion,\n" +
                "    b.qty_favorites AS LDL_collection_quantity,\n" +
                "    b.review_count AS LDL_total_comments,\n" +
                "    b.price AS LDL_price,\n" +
                "    b.price_marked AS LDL_original_price,\n" +
                "    b.price_avg AS LDL_transaction_price,\n" +
                "    b.qty AS LDL_qty,\n" +
                "    IF(b.val_sales_amount = 0, b.price * b.qty, b.val_sales_amount) AS LDL_value,\n" +
                "    CASE\n" +
                "        WHEN b.grand = 'true' THEN '天猫'\n" +
                "        WHEN b.grand = 'false' THEN '淘宝'\n" +
                "        ELSE ''\n" +
                "    END AS LDL_platform,\n" +
                "    a.source AS pt_channel,\n" +
                "    a.pt_ym AS pt_ym,\n" +
                "    '' AS DY_id,\n" +
                "    '' AS DY_itemname,\n" +
                "    '' AS DY_categoryname,\n" +
                "    '' AS DY_subname,\n" +
                "    '' AS DY_brandname,\n" +
                "    '' AS DY_pricetext,\n" +
                "    '' AS DY_yjrate,\n" +
                "    '' AS DY_shopname,\n" +
                "    '' AS DY_url,\n" +
                "    '' AS DY_detail_link,\n" +
                "    '' AS DY_img_link,\n" +
                "    '' AS DY_salescount,\n" +
                "    '' AS DY_sales,\n" +
                "    '' AS DY_price,\n" +
                "    '' AS DY_lookcount,\n" +
                "    '' AS DY_livecount,\n" +
                "    '' AS DY_videocount,\n" +
                "    '' AS DY_conversion_rate,\n" +
                "    '' AS DY_platname,\n" +
                "    '' AS DY_BloggerCount,\n" +
                "    '' AS DY_CosRatio,\n" +
                "    '' AS DY_createdate,\n" +
                "    '' AS DY_adddate,\n" +
                "    '' AS DY_str_categoryid,\n" +
                "    '' AS DY_category\n" +
                "FROM (SELECT *, 'source4' AS source FROM dwd.source4 WHERE pt_channel IN ('taobao', 'tmall') AND pt_ym = ? ) a\n" +
                "LEFT JOIN dwd.ldl b ON a.pt_ym = b.pt_ym AND a.goods_id = b.itemid;";

        // 执行SQL语句
        try (Connection connection = DriverManager.getConnection(url, user, password);
             Statement stmt = connection.createStatement()) {

            // 设置超时时间
            stmt.execute("SET receive_timeout = 3600");
            stmt.execute("SET send_timeout = 3600");

            // 执行插入语句
            try (PreparedStatement pstmt = connection.prepareStatement(insertSQL)) {
                for (String ptYm : ptYmValues) {
                    pstmt.setString(1, ptYm);  // 设置第一个 pt_ym 值
                    pstmt.setString(2, ptYm);  // 设置第二个 pt_ym 值

                    pstmt.executeUpdate();  // 执行插入操作
                }
                System.out.println("Data inserted successfully for pt_ym values: " + ptYmValues);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
