package from_csv.aindustry_brand;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.nio.charset.StandardCharsets;

public class model {

    private static final String API_KEY = "sk-0829b0dbec1d4415bacc60cc6925ac18";
    private static final String API_URL = "https://dashscope.aliyuncs.com/api/v1/services/aigc/multimodal-generation/generation";

    public static void main(String[] args) {
        String imagePath = "D:\\wzza\\develop\\idea_project\\ceshi\\src\\main\\java\\from_csv\\aindustry_brand\\Image.png";
        String outputCsv = "sales_data.csv";

        try {
            // 检查图片文件是否存在
            File imageFile = new File(imagePath);
            if (!imageFile.exists()) {
                System.err.println("错误：图片文件不存在: " + imagePath);
                return;
            }
            
            System.out.println("正在提取图片中的表格数据...");
            String markdownText = extractTextFromImage(imagePath);
            System.out.println("原始Markdown数据:\n" + markdownText);

            // 检查是否成功提取到文本
            if (markdownText == null || markdownText.trim().isEmpty()) {
                System.err.println("错误：未能从图片中提取到文本数据");
                return;
            }

            System.out.println("\n解析表格数据...");
            List<String> headers = new ArrayList<>();
            List<List<String>> tableData = new ArrayList<>();
            parseMarkdownTables(markdownText, headers, tableData);

            if (tableData.isEmpty()) {
                System.err.println("警告：未能解析到任何表格数据");
                return;
            }

            System.out.println("\n处理后的数据样例:");
            for (int i = 0; i < Math.min(3, tableData.size()); i++) {
                System.out.println(tableData.get(i));
            }

            saveToCsv(headers, tableData, outputCsv);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static String extractTextFromImage(String imagePath) throws Exception {
        // 读取图片并转换为Base64
        byte[] imageBytes = Files.readAllBytes(Paths.get(imagePath));
        String base64Image = Base64.getEncoder().encodeToString(imageBytes);

        // 构建请求体
        String requestBody = String.format(
                "{\"model\":\"qwen-vl-max\",\"input\":{\"messages\":[{\"role\":\"user\",\"content\":[{\"image\":\"data:image/jpeg;base64,%s\"},{\"text\":\"请提取图片中的所有表格数据，用规范的Markdown表格格式返回，包含表头\"}]}]}}",
                base64Image
        );

        // 使用Apache HttpClient发送请求
        try (CloseableHttpClient client = HttpClients.createDefault()) {
            HttpPost post = new HttpPost(API_URL);
            post.setHeader("Authorization", "Bearer " + API_KEY);
            post.setHeader("Content-Type", "application/json; charset=UTF-8");
            post.setEntity(new StringEntity(requestBody, "UTF-8"));

            try (CloseableHttpResponse response = client.execute(post)) {
                HttpEntity entity = response.getEntity();
                String responseBody = EntityUtils.toString(entity, "UTF-8");
                
                System.out.println("API响应: " + responseBody); // 调试输出
                
                // 解析阿里云通义千问的响应格式
                // 响应格式: {"output":{"choices":[{"message":{"content":[{"text":"..."}]}}]}}
                
                // 1. 找到 "text": 字段
                String[] textSplit = responseBody.split("\"text\":\"");
                if (textSplit.length < 2) {
                    throw new Exception("API响应格式错误，未找到text字段: " + responseBody);
                }
                
                // 2. 提取text内容，处理转义字符
                String textContent = textSplit[1];
                
                // 3. 找到text内容的结束位置（下一个双引号，但要考虑转义）
                StringBuilder result = new StringBuilder();
                boolean escaped = false;
                
                for (int i = 0; i < textContent.length(); i++) {
                    char c = textContent.charAt(i);
                    
                    if (escaped) {
                        // 处理转义字符
                        switch (c) {
                            case 'n':
                                result.append('\n');
                                break;
                            case '"':
                                result.append('"');
                                break;
                            case '\\':
                                result.append('\\');
                                break;
                            default:
                                result.append(c);
                                break;
                        }
                        escaped = false;
                    } else if (c == '\\') {
                        escaped = true;
                    } else if (c == '"') {
                        // 遇到未转义的引号，结束
                        break;
                    } else {
                        result.append(c);
                    }
                }
                
                String finalResult = result.toString();
                
                // 处理Unicode转义字符 (如 \u4e2d -> 中)
                finalResult = decodeUnicodeEscapes(finalResult);
                
                return finalResult;
            }
        }
    }
    
    // 解码Unicode转义字符的辅助方法
    private static String decodeUnicodeEscapes(String text) {
        StringBuilder result = new StringBuilder();
        int i = 0;
        while (i < text.length()) {
            if (i + 5 < text.length() && text.charAt(i) == '\\' && text.charAt(i + 1) == 'u') {
                try {
                    String unicode = text.substring(i + 2, i + 6);
                    int codePoint = Integer.parseInt(unicode, 16);
                    result.append((char) codePoint);
                    i += 6;
                } catch (NumberFormatException e) {
                    result.append(text.charAt(i));
                    i++;
                }
            } else {
                result.append(text.charAt(i));
                i++;
            }
        }
        return result.toString();
    }

    public static void parseMarkdownTables(String markdownText, List<String> headers, List<List<String>> tableData) throws IOException {
        if (markdownText == null || markdownText.isEmpty()) {
            return;
        }

        System.out.println("开始解析Markdown表格...");

        // 分割不同平台的表格
        String[] platformSections = markdownText.split("### ");

        for (int i = 0; i < platformSections.length; i++) {
            String section = platformSections[i].trim();
            if (section.isEmpty()) {
                continue;
            }

            System.out.println("\n处理第" + i + "个section: " + section.substring(0, Math.min(50, section.length())) + "...");

            // 提取平台名称
            Matcher platformMatcher = Pattern.compile("^([^\n]+)").matcher(section);
            if (!platformMatcher.find()) {
                System.out.println("未找到平台名称，跳过");
                continue;
            }
            String platform = platformMatcher.group(1).trim();
            System.out.println("平台名称: " + platform);

            // 提取表格数据 - 简化的匹配方式
            String[] lines = section.split("\n");
            List<String> tableLines = new ArrayList<>();
            boolean inTable = false;
            
            for (String line : lines) {
                line = line.trim();
                if (line.startsWith("|") && line.endsWith("|")) {
                    tableLines.add(line);
                    inTable = true;
                } else if (inTable && !line.startsWith("|")) {
                    // 表格结束
                    break;
                }
            }
            
            System.out.println("找到表格行数: " + tableLines.size());
            
            if (tableLines.size() >= 3) { // 至少要有表头、分隔线、数据行
                // 构建表格字符串
                String tableDataStr = String.join("\n", tableLines) + "\n";

                // 手动解析Markdown表格
                System.out.println("解析表格数据...");
                
                boolean isFirstTable = headers.isEmpty();
                int rowCount = 0;
                
                for (String line : tableLines) {
                    line = line.trim();
                    if (line.startsWith("|") && line.endsWith("|")) {
                        // 移除首尾的|并分割
                        String content = line.substring(1, line.length() - 1);
                        String[] cells = content.split("\\|");
                        
                        // 清理每个单元格
                        for (int j = 0; j < cells.length; j++) {
                            cells[j] = cells[j].trim();
                        }
                        
                        if (rowCount == 0 && isFirstTable) {
                            // 第一个表格的表头
                            headers.clear();
                            headers.add("平台"); // 添加平台列
                            for (String cell : cells) {
                                if (!cell.isEmpty()) {
                                    headers.add(cell);
                                }
                            }
                            System.out.println("表头: " + headers);
                        } else if (rowCount == 1) {
                            // 跳过分隔线
                            System.out.println("跳过分隔线");
                        } else if (cells.length >= 4) {
                            // 数据行
                            List<String> rowData = new ArrayList<>();
                            rowData.add(platform); // 添加平台名
                            for (String cell : cells) {
                                if (!cell.isEmpty() && !cell.matches("[-:|]+")) {
                                    rowData.add(cell);
                                }
                            }
                            if (rowData.size() >= 5) { // 平台 + 至少4列数据
                                tableData.add(rowData);
                                System.out.println("添加数据行: " + rowData);
                            }
                        }
                        rowCount++;
                    }
                }
            } else {
                System.out.println("表格行数不足，跳过");
            }
        }
    }

    public static void saveToCsv(List<String> headers, List<List<String>> data, String outputPath) throws IOException {
        // 添加平台列到表头
        List<String> fullHeaders = new ArrayList<>();
        fullHeaders.add("平台");
        fullHeaders.addAll(headers);

        try (OutputStreamWriter writer = new OutputStreamWriter(new FileOutputStream(outputPath), "UTF-8");
             CSVPrinter csvPrinter = new CSVPrinter(writer,
                     CSVFormat.DEFAULT
                             .withHeader(fullHeaders.toArray(new String[0]))
                             .withIgnoreEmptyLines(true))) {

            for (List<String> row : data) {
                csvPrinter.printRecord(row);
            }

            System.out.println("结果已保存到: " + new File(outputPath).getAbsolutePath());
        }
    }
}