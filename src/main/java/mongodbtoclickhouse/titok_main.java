package mongodbtoclickhouse;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

public class titok_main {

    public static void main(String[] args) {
        String clickhouseTable1 = "feigua_goods_overview_stat";
        String clickhouseUrl = "jdbc:clickhouse://hadoop110:8123";
        String mongodbbase = "sc";
        String mongodbtable = "feigua_goods_overview_stat_20240805_20240805";
        String clickhousedatabase = "ods";
        String clickhouseTable = clickhousedatabase + "." + clickhouseTable1;



        tiktok.mongotock(clickhouseTable,clickhouseUrl,mongodbtable,mongodbbase);
    }





}
