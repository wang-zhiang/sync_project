package mongodbtoclickhouse;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

public class main {
    public static void main(String[] args) {




        String clickhouseTable1 = "feigua_dy_month_all_cate_xpl";
        String clickhouseUrl = "jdbc:clickhouse://192.168.5.111:8123";
        String mongodbbase = "sc";
        String clickhousedatabase = "ods";
        String clickhouseTable = clickhousedatabase + "." + clickhouseTable1;
        String csvFile = "D:\\wzza\\develop\\idea_project\\ceshi\\src\\main\\java\\mongodbtoclickhouse\\a.csv";

        String line = "";
        String csvSplitBy = ",";

        try (BufferedReader br = new BufferedReader(new FileReader(csvFile))) {
            while ((line = br.readLine()) != null) {
                StringBuilder sb = new StringBuilder();
                String[] data = line.split(csvSplitBy);
                for (String value : data) {
                    String sqltable =value.trim();
                    System.out.println("开始同步表：" + sqltable);
                    mongotock.mongotock(clickhouseTable,clickhouseUrl,value,mongodbbase);
                    break;

                }

            }
        } catch (IOException e) {
            e.printStackTrace();
        }




    }
}
