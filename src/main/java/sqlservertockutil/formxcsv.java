package sqlservertockutil;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;


public class formxcsv {





        public static void main(String[] args) {

            System.setProperty("https.protocols", "TLSv1.2");
            // 指定要读取的CSV文件路径

//            String sqlServerUsername = "CHH";
//            String sqlServerPassword = "Y1v606";
            String sqlServerUrl = "jdbc:sqlserver://192.168.4.39;DatabaseName=trading_medicine";

            String sqlServerUsername = "sa";
            String sqlServerPassword = "smartpthdata";

            String csvFilePath = "D:\\wzza\\develop\\idea_project\\ceshi\\src\\main\\java\\sqlservertockutil\\a.xlsx";

            try (BufferedReader br = new BufferedReader(new FileReader(csvFilePath))) {
                String line;
                while ((line = br.readLine()) != null) {
                    //String sqltable =line.trim() + "ty";
                    String sqltable =line.trim() ;
                    String cktable = "dwd." + line;
                    System.out.println(sqltable + cktable);
                   // String conf = sql2_ck.getConf(cktable, sqltable, sqlServerUsername, sqlServerPassword);
                    String conf = mssql_ck2_12.getConf(cktable, sqltable, sqlServerUrl, sqlServerUsername, sqlServerPassword);
                    System.out.println("正在同步：" + sqltable);
                    System.out.println("开始下一轮：");
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }




}
