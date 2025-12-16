package sibo;

import java.util.List;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import java.io.BufferedReader;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class sql_to_ck_tablename {
    public static void main(String[] args) {


        String type = "tm"; // 或者 "b2c" //tm
        String type1 = "";


        if (type.equals("tm")) {
            type1 = "tm";
        } else if (type.equals("b2c")) {
            type1 = "b2c";
        }

        List<String> tableNames = null;
        try {
            tableNames = readCsvFile("D:\\wzza\\develop\\idea_project\\ceshi\\src\\main\\java\\sibo\\a1.csv");
        } catch (IOException e) {
            e.printStackTrace();
        }
        for (String tableName : tableNames) {
            String s = tableName.toLowerCase();
            System.out.println("new_ec_"+ type1 +"_his_" + s);




        }


    }


    private static List<String> readCsvFile(String filePath) throws IOException {
        List<String> tableNames = new ArrayList<>();
        try (BufferedReader br = new BufferedReader(new FileReader(filePath))) {
            String line;
            while ((line = br.readLine()) != null) {
                if (!line.trim().isEmpty()) {
                    tableNames.add(line);
                }
            }
        }
        return tableNames;
    }

}