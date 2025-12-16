package elm_mt_filter;
import com.jcraft.jsch.*;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.InputStream;
import java.io.OutputStream;

public class ssh {

    public static void main(String[] args) {
        executeCommand("clickhouse-client --password smartpath --port 9090 --query=\"create table test.elm_filt  ENGINE = MergeTree order by  Province AS\n" +
                "select\n" +
                "province Province,\n" +
                "city City,\n" +
                "'' ShopName,\n" +
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
                "'' UpdateTime,\n" +
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
                "'感冒咳嗽' industryId,\n" +
                "'' ADDDATE,\n" +
                "'' ser_num,\n" +
                "'' seriesid,\n" +
                "'' skuid,\n" +
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
                "from (select * from  ods.ods_elm where pt_ym = '202403' and mark = 'eleme') where itemname LIKE '%柴桂退热颗粒%' OR itemname LIKE '%感冒素%' OR itemname LIKE '%咳嗽颗粒%' OR itemname LIKE '%止咳%' OR itemname LIKE '%退烧%' OR itemname LIKE '%愈酚伪麻%' OR itemname LIKE '%阿酚咖敏片%' OR itemname LIKE '%氨酚黄%' OR itemname LIKE '%氨酚咖黄烷胺片%' OR itemname LIKE '%氨酚咖那敏片%' OR itemname LIKE '%氨酚麻美%' OR itemname LIKE '%氨酚那敏三味浸膏胶囊%' OR itemname LIKE '%氨酚曲麻片%' OR itemname LIKE '%氨酚烷胺%' OR itemname LIKE '%氨酚伪麻%' OR itemname LIKE '%氨金黄敏%' OR itemname LIKE '%氨咖%' OR itemname LIKE '%氨麻美敏%' OR itemname LIKE '%百咳素%' OR itemname LIKE '%贝敏伪麻片%' OR itemname LIKE '%布洛伪麻%' OR itemname LIKE '%儿童复方氨酚肾素片%' OR itemname LIKE '%酚咖麻%' OR itemname LIKE '%酚麻美%' OR itemname LIKE '%酚美愈伪麻分散片%' OR itemname LIKE '%复方氨酚%' OR itemname LIKE '%北豆根氨酚那敏片%' OR itemname LIKE '%贝母氯化铵片%' OR itemname LIKE '%复方酚咖%' OR itemname LIKE '%复方甘草%' OR itemname LIKE '%复方桔梗麻%' OR itemname LIKE '%磷酸可待因%' OR itemname LIKE '%氢溴酸右美沙芬%' OR itemname LIKE '%复方锌布颗粒%' OR itemname LIKE '%银翘氨敏胶囊%' OR itemname LIKE '%创木酚磺酸钾%' OR itemname LIKE '%愈酚喷托那敏%' OR itemname LIKE '%甘草浙贝%' OR itemname LIKE '%感冒%' OR itemname LIKE '%枸橼酸喷托维林%' OR itemname LIKE '%咳哌宁%' OR itemname LIKE '%化痰素%' OR itemname LIKE '%黄敏胶囊%' OR itemname LIKE '%黄烷胺片%' OR itemname LIKE '%咖酚伪麻片%' OR itemname LIKE '%可泰舒氨酚咖那敏片%' OR itemname LIKE '%磷酸苯丙哌林%' OR itemname LIKE '%羚黄氨咖敏片%' OR itemname LIKE '%柳酚咖敏片%' OR itemname LIKE '%洛芬葡锌那敏片%' OR itemname LIKE '%美敏伪麻口服%' OR itemname LIKE '%美敏伪麻%' OR itemname LIKE '%那可丁片%' OR itemname LIKE '%那可丁糖浆%' OR itemname LIKE '%那敏颗粒%' OR itemname LIKE '%喷托维林氯化铵%' OR itemname LIKE '%双分伪麻胶囊%' OR itemname LIKE '%双分伪麻胶囊/酚麻美敏胶囊%' OR itemname LIKE '%酸氨溴索口服溶液%' OR itemname LIKE '%羧甲司坦%' OR itemname LIKE '%唯金诺氨酚咖那敏片%' OR itemname LIKE '%氨酚黄那敏%' OR itemname LIKE '%氨酚烷胺颗粒%' OR itemname LIKE '%氨咖黄敏颗粒%' OR itemname LIKE '%布洛伪麻分散片%' OR itemname LIKE '%复方氨酚烷胺片%' OR itemname LIKE '%愈美那敏溶液%' OR itemname LIKE '%盐酸氨溴索%' OR itemname LIKE '%盐酸氯哌丁片%' OR itemname LIKE '%盐酸美司坦片%' OR itemname LIKE '%盐酸溴己新片%' OR itemname LIKE '%乙酰半胱氨%' OR itemname LIKE '%银翘氨敏%' OR itemname LIKE '%右美沙芬%' OR itemname LIKE '%愈创甘油%' OR itemname LIKE '%愈酚喷托异丙嗪%' OR itemname LIKE '%愈酚维林片%' OR itemname LIKE '%愈酚溴新口服溶液%' OR itemname LIKE '%愈美%' OR itemname LIKE '%愈美那敏%' OR itemname LIKE '%愈美颗粒%' OR itemname LIKE '%愈创甘油醚糖浆%' OR itemname LIKE '%右美沙芬愈创甘油醚%' OR itemname LIKE '%复方愈创木酚磺酸钾%' OR itemname LIKE '%复方银翘氨敏%' OR itemname LIKE '%复方锌布%' OR itemname LIKE '%复方氢溴酸右美沙芬%' OR itemname LIKE '%复方甘草浙贝氯化铵片%' OR itemname LIKE '%复方北豆根氨酚那敏片%' OR itemname LIKE '%复方氨酚溴敏%' OR itemname LIKE '%复方氨酚烷%' OR itemname LIKE '%复方氨酚肾素片%' OR itemname LIKE '%复方氨酚那敏%' OR itemname LIKE '%氨咖愈敏溶液%' OR itemname LIKE '%氨咖黄敏%' OR itemname LIKE '%氨酚咖那敏%' OR itemname LIKE '%咳嗽%' OR itemname LIKE '%咳特灵%' OR itemname LIKE '%肺宁颗粒%' OR itemname LIKE '%金银花颗粒%' OR itemname LIKE '%咳喘灵%' OR itemname LIKE '%板蓝根%' OR itemname LIKE '%贝母%' OR itemname LIKE '%清开灵%' OR itemname LIKE '%清热解毒%' OR itemname LIKE '%罗汉果%' OR itemname LIKE '%润肺止嗽丸%' OR itemname LIKE '%感愈胶囊%' OR itemname LIKE '%退热颗粒%' OR itemname LIKE '%痰咳净片%' OR itemname LIKE '%荆防颗粒%' OR itemname LIKE '%橘红丸%' OR itemname LIKE '%双黄连口服液%' OR itemname LIKE '%清肺糖浆%' OR itemname LIKE '%银翘解毒%' OR itemname LIKE '%橘红痰咳颗粒%' OR itemname LIKE '%咳喘颗粒%' OR itemname LIKE '%枇杷片%' OR itemname LIKE '%雪梨膏%' OR itemname LIKE '%肺宁胶囊%' OR itemname LIKE '%枇杷膏%' OR itemname LIKE '%十五味龙胆花丸%' OR itemname LIKE '%夏桑菊颗粒%' OR itemname LIKE '%橘红片%' OR itemname LIKE '%枇杷露%' OR itemname LIKE '%银翘片%' OR itemname LIKE '%解表颗粒%' OR itemname LIKE '%金感胶囊%' OR itemname LIKE '%咳速停糖浆%' OR itemname LIKE '%川贝糖浆%' OR itemname LIKE '%连花清瘟胶囊%' OR itemname LIKE '%姜枣祛寒颗粒%' OR itemname LIKE '%橘红颗粒%' OR itemname LIKE '%清热消炎宁胶囊%' OR itemname LIKE '%解毒颗粒%' OR itemname LIKE '%肺宁片%' OR itemname LIKE '%柴胡颗粒%' OR itemname LIKE '%清肺胶囊%' OR itemname LIKE '%参苏丸%' OR itemname LIKE '%痰咳净%' OR itemname LIKE '%二陈丸%' OR itemname LIKE '%凉茶颗粒%' OR itemname LIKE '%润肺膏%' OR itemname LIKE '%小柴胡汤丸%' OR itemname LIKE '%清气化痰丸%' OR itemname LIKE '%柴黄颗粒%' OR itemname LIKE '%百合固金片%' OR itemname LIKE '%连花清瘟颗粒%' OR itemname LIKE '%通宣理肺颗粒%' OR itemname LIKE '%复方穿心莲片%' OR itemname LIKE '%桂枝颗粒%' OR itemname LIKE '%沙棘颗粒%' OR itemname LIKE '%清咽颗粒%' OR itemname LIKE '%蛤蚧定喘丸%' OR itemname LIKE '%伤风停胶囊%' OR itemname LIKE '%咳喘丸%' OR itemname LIKE '%清肺片%' OR itemname LIKE '%镇咳糖浆%' OR itemname LIKE '%热炎宁颗粒%' OR itemname LIKE '%正柴胡饮颗粒%' OR itemname LIKE '%咳清胶囊%' OR itemname LIKE '%牛黄清肺散%' OR itemname LIKE '%清热颗粒%' OR itemname LIKE '%解感颗粒%' OR itemname LIKE '%咳速停胶囊%' OR itemname LIKE '%羚翘解毒丸%' OR itemname LIKE '%克咳片%' OR itemname LIKE '%蛇胆陈皮胶囊%' OR itemname LIKE '%罗汉果玉竹颗粒%' OR itemname LIKE '%热速清颗粒%' OR itemname LIKE '%桔贝合剂%' OR itemname LIKE '%抗病毒%' OR itemname LIKE '%咳喘胶囊%' OR itemname LIKE '%百咳静%' OR itemname LIKE '%芎菊上清丸%' OR itemname LIKE '%复方一枝蒿颗粒%' OR itemname LIKE '%葶贝胶囊%' OR itemname LIKE '%止嗽合剂%' OR itemname LIKE '%清肺颗粒%' OR itemname LIKE '%通宣理肺片%' OR itemname LIKE '%夏桑菊凉茶%' OR itemname LIKE '%百合固金丸%' OR itemname LIKE '%复方鲜竹沥液%' OR itemname LIKE '%通宣理肺丸%' OR itemname LIKE '%热速清%' OR itemname LIKE '%清肺化痰颗粒%' OR itemname LIKE '%气管炎丸%' OR itemname LIKE '%扑感片%' OR itemname LIKE '%清肺化痰丸%' OR itemname LIKE '%川贝散%' OR itemname LIKE '%橘红痰咳液%' OR itemname LIKE '%咳舒糖浆%' OR itemname LIKE '%橘红痰咳煎膏%' OR itemname LIKE '%清热灵%' OR itemname LIKE '%补肺丸%' OR itemname LIKE '%养肺丸%' OR itemname LIKE '%儿童咳液%' OR itemname LIKE '%肺力咳合剂%' OR itemname LIKE '%急支颗粒%' OR itemname LIKE '%银翘散%' OR itemname LIKE '%急支糖浆%' OR itemname LIKE '%百合固金口服液%' OR itemname LIKE '%小柴胡片%' OR itemname LIKE '%虫草川贝膏%' OR itemname LIKE '%柴银口服液%' OR itemname LIKE '%通宣理肺胶囊%' OR itemname LIKE '%陈皮散%' OR itemname LIKE '%风热清合剂%' OR itemname LIKE '%风热清口服液%' OR itemname LIKE '%克咳胶囊%' OR itemname LIKE '%桑菊银翘散%' OR itemname LIKE '%宝泰康颗粒%' OR itemname LIKE '%艾叶油软胶囊%' OR itemname LIKE '%咳喘宁%' OR itemname LIKE '%银花芒果胶囊%' OR itemname LIKE '%沙棘糖浆%' OR itemname LIKE '%咳露口服液%' OR itemname LIKE '%咳克平胶囊%' OR itemname LIKE '%感通片%' OR itemname LIKE '%健肺丸%' OR itemname LIKE '%解表口服液%' OR itemname LIKE '%咳平胶囊%' OR itemname LIKE '%清毒糖浆%' OR itemname LIKE '%疏风散热胶囊%' OR itemname LIKE '%金莲清热颗粒%' OR itemname LIKE '%银翘伤风胶囊%' OR itemname LIKE '%疏风解毒胶囊%' OR itemname LIKE '%蛤蚧定喘胶囊%' OR itemname LIKE '%复方罗汉果清肺颗粒%' OR itemname LIKE '%清肺消炎丸%' OR itemname LIKE '%清肺宁嗽丸%' OR itemname LIKE '%柴胡滴丸%' OR itemname LIKE '%咳欣康片%' OR itemname LIKE '%止嗽立效胶囊%' OR itemname LIKE '%双清颗粒%' OR itemname LIKE '%青龙颗粒%' OR itemname LIKE '%除痰止嗽丸%' OR itemname LIKE '%二母宁嗽%' OR itemname LIKE '%洋参保肺丸%' OR itemname LIKE '%芎菊上清颗粒%' OR itemname LIKE '%黄花杜鹃油%' OR itemname LIKE '%止嗽化痰颗粒%' \"");
    }

    public static void executeCommand(String command) {
        String host = "192.168.5.110";
        String user = "smartpath";
        String password = "cl@1109!@#";
        int port = 22;
        String keyword = "smartpath@hadoop110";
        int bufferSize = 100*1024; // 设置缓冲区大小为 8KB

        try {
            // 关闭JSch的日志输出
            JSch.setLogger(new Logger() {
                @Override
                public boolean isEnabled(int level) {
                    return false; // 禁用日志
                }

                @Override
                public void log(int level, String message) {
                    // 不执行任何操作
                }
            });

            JSch jsch = new JSch();
            Session session = jsch.getSession(user, host, port);
            session.setPassword(password);
            session.setConfig("StrictHostKeyChecking", "no");
            session.connect();

            Channel channel = session.openChannel("shell");
            channel.connect();

            // 使用 BufferedOutputStream 和 BufferedInputStream 包装输出和输入流，并设置缓冲区大小
            OutputStream out = new BufferedOutputStream(channel.getOutputStream(), bufferSize);
            InputStream in = new BufferedInputStream(channel.getInputStream(), bufferSize);

            // 发送要执行的命令
            String fullCommand = command + "\n";
            out.write(fullCommand.getBytes());
            out.flush();

            // 读取输出，不打印，只处理
            byte[] buffer = new byte[bufferSize];
            int bytesRead;
            boolean keywordFound = false;
            boolean firstPromptFound = false;
            while ((bytesRead = in.read(buffer)) != -1) {
                String output = new String(buffer, 0, bytesRead);
                if (!firstPromptFound && output.contains(keyword)) {
                    firstPromptFound = true; // 忽略首次出现的命令提示符
                    continue;
                }
                // 此处不再输出，只检查关键词
                if (output.contains(keyword)) {
                    break; // 如果关键词出现，结束循环
                }
            }

            out.close();
            in.close();
            channel.disconnect();
            session.disconnect();
        } catch (JSchException | java.io.IOException e) {
            e.printStackTrace();
        }
    }

}
