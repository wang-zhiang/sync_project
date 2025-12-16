package sqlservertockutil;

import au.com.bytecode.opencsv.CSVReader;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

public class csvmain {

    public static void main(String[] args) {

//        String  cktable = "dwd.ceshi";
//        String sqltable ="TradingYaoPinhy1412021";

        String sqlServerUrl = "jdbc:sqlserver://192.168.4.39;DatabaseName=trading_medicine";
        //String sqlServerUrl = "jdbc:sqlserver://192.168.4.201:2422;DatabaseName=Taobao_trading";
        String sqlServerUsername = "sa";
        String sqlServerPassword = "smartpthdata";

        String csvFile = "D:\\wzza\\develop\\idea_project\\ceshi\\src\\main\\java\\sqlservertockutil\\a.xlsx";

        String line = "";
        String csvSplitBy = ",";

        try (BufferedReader br = new BufferedReader(new FileReader(csvFile))) {
            while ((line = br.readLine()) != null) {
                StringBuilder sb = new StringBuilder();
                String[] data = line.split(csvSplitBy);
//                for (String value : data) {
//                    String sqltable =value.trim();
//                    String cktable = "dwd." + sqltable;
//                    System.out.println(sqltable + cktable);
//                    //String conf = mssql_ck1.getConf(cktable, sqltable, sqlServerUrl, sqlServerUsername, sqlServerPassword);
//                    System.out.println("开始下一轮：");
//                    break;
//
//                }


                    String sqltable =data[0].trim();
                    String cktable = "dwd." + data[1];
                    System.out.println(sqltable + cktable);
                    String conf = mssql_ck2_12.getConf(cktable, sqltable, sqlServerUrl, sqlServerUsername, sqlServerPassword);
                    System.out.println("开始下一轮：");
                    //break一开始是有的被我注释掉了，不太清楚放在这里的作用，但目前发现阻碍了循环
                    //break;



            }
        } catch (IOException e) {
            e.printStackTrace();
        }


    }
}
