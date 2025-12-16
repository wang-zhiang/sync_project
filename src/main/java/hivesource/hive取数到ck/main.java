package hivesource.hive取数到ck;

import hivesource.connhive;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

public class main {

    public static void main(String[] args) {



//        String clickhouseTable = "tm_sku_price_month";
//        String clickhouseUrl = "jdbc:clickhouse://hadoop110:8123/ods";
//        String mongodbbase = "ec";

        String csvFile = "D:\\wzza\\develop\\idea_project\\ceshi\\src\\main\\java\\hivesource\\hive取数到ck\\a.csv";

        String line = "";
        String csvSplitBy = ",";

        try (BufferedReader br = new BufferedReader(new FileReader(csvFile))) {
            while ((line = br.readLine()) != null) {
                StringBuilder sb = new StringBuilder();
                String[] data = line.split(csvSplitBy);
                for (String value : data) {
                    String hivetable =value.trim();
                    //System.out.println("开始同步表：" + sqltable);
                    get_data_to_ck.get_uaually_hive(hivetable);
                    break;

                }

            }
        } catch (IOException e) {
            e.printStackTrace();
        }




    }
}
