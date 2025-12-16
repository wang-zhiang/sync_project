package sibo.tmp;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class CsvMatcher {

    public static void main(String[] args) {
        String aCsvPath = "D:\\wzza\\develop\\idea_project\\ceshi\\src\\main\\java\\sibo\\tmp\\a.csv"; // 更改为a.csv文件的路径
        String bCsvPath = "D:\\wzza\\develop\\idea_project\\ceshi\\src\\main\\java\\sibo\\tmp\\b.csv"; // 更改为b.csv文件的路径

        List<String> aList = readCsvFile(aCsvPath);
        List<String> bList = readCsvFile(bCsvPath);

        for (String a : aList) {
            for (String b : bList) {
                if (b.toLowerCase().contains(a.toLowerCase())) {
                    System.out.println(b);
                    break; // 假设一旦找到包含a的b，就停止检查其他的b
                }
            }
        }
    }

    private static List<String> readCsvFile(String filePath) {
        List<String> list = new ArrayList<>();
        try (BufferedReader br = new BufferedReader(new FileReader(filePath))) {
            String line;
            while ((line = br.readLine()) != null) {
                if (!line.trim().isEmpty()) {
                    list.add(line.trim());
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return list;
    }
}
