package from_csv.aindustry_brand;
import org.apache.poi.hssf.usermodel.*;
import org.apache.poi.ss.usermodel.*;
import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.util.*;
import java.util.regex.Pattern;
import java.util.regex.Matcher;
import java.util.Base64;
import java.nio.file.Files;

public class CloudOCRParser {
    
    // 百度OCR API配置 - 需要你自己申请
    private static final String API_KEY = "你的API_KEY";
    private static final String SECRET_KEY = "你的SECRET_KEY";
    private static final String OCR_URL = "https://aip.baidubce.com/rest/2.0/ocr/v1/general_basic";
    private static final String TOKEN_URL = "https://aip.baidubce.com/oauth/2.0/token";
    
    private String accessToken;
    
    public static void main(String[] args) {
        try {
            CloudOCRParser parser = new CloudOCRParser();
            String filePath = "D:\\wzza\\develop\\idea_project\\ceshi\\src\\main\\java\\from_csv\\aindustry_brand\\brand\\2025-1品牌榜单\\3c数码\\3c数码配件.xls";
            parser.processExcelFile(filePath);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
    public CloudOCRParser() {
        // 获取百度API访问token
        this.accessToken = getAccessToken();
    }
    
    public void processExcelFile(String filePath) throws IOException {
        System.out.println("开始使用云端OCR解析Excel文件: " + filePath);
        
        FileInputStream fis = new FileInputStream(filePath);
        HSSFWorkbook workbook = new HSSFWorkbook(fis);
        
        String fileName = new File(filePath).getName().replace(".xls", "");
        
        // 创建输出目录
        File outputDir = new File("cloud_extracted_" + fileName);
        if (!outputDir.exists()) {
            outputDir.mkdirs();
        }
        
        List<BrandRankingEntry> allEntries = new ArrayList<>();
        int totalImages = 0;
        
        // 扫描所有工作表
        for (int sheetIndex = 0; sheetIndex < workbook.getNumberOfSheets(); sheetIndex++) {
            HSSFSheet sheet = workbook.getSheetAt(sheetIndex);
            String sheetName = sheet.getSheetName();
            System.out.println("\n处理工作表: " + sheetName);
            
            HSSFPatriarch patriarch = sheet.getDrawingPatriarch();
            if (patriarch != null) {
                List<HSSFShape> shapes = patriarch.getChildren();
                int imageCount = 0;
                
                for (HSSFShape shape : shapes) {
                    if (shape instanceof HSSFPicture) {
                        imageCount++;
                        totalImages++;
                        
                        HSSFPicture picture = (HSSFPicture) shape;
                        
                        try {
                            // 提取图片
                            byte[] imageData = picture.getPictureData().getData();
                            BufferedImage image = ImageIO.read(new ByteArrayInputStream(imageData));
                            
                            if (image != null) {
                                System.out.println("\n  === 云端OCR解析图片 " + totalImages + " ===");
                                
                                // 保存图片
                                String imageName = String.format("%s_image_%d.png", fileName, totalImages);
                                File imageFile = new File(outputDir, imageName);
                                ImageIO.write(image, "PNG", imageFile);
                                System.out.println("  图片已保存: " + imageFile.getName());
                                
                                // 调用云端OCR
                                String ocrText = performCloudOCR(imageFile);
                                System.out.println("  云端OCR识别结果:");
                                System.out.println("  " + ocrText.replace("\n", "\n  "));
                                
                                // 智能解析数据
                                List<BrandRankingEntry> entries = parseRankingData(ocrText, totalImages, imageName, fileName);
                                System.out.println("  解析到 " + entries.size() + " 条数据");
                                allEntries.addAll(entries);
                            }
                        } catch (Exception e) {
                            System.err.println("  处理图片 " + imageCount + " 失败: " + e.getMessage());
                        }
                    }
                }
            }
        }
        
        System.out.println("\n============================================================");
        System.out.println("解析完成！总图片数: " + totalImages + ", 总数据条数: " + allEntries.size());
        
        // 只生成CSV文件
        generateCSVFile(allEntries, fileName);
        
        workbook.close();
        fis.close();
    }
    
    private String getAccessToken() {
        try {
            String tokenUrl = TOKEN_URL + "?grant_type=client_credentials" +
                             "&client_id=" + API_KEY +
                             "&client_secret=" + SECRET_KEY;
            
            URL url = new URL(tokenUrl);
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("POST");
            conn.setRequestProperty("Content-Type", "application/json");
            
            BufferedReader reader = new BufferedReader(new InputStreamReader(conn.getInputStream()));
            String line;
            StringBuilder response = new StringBuilder();
            while ((line = reader.readLine()) != null) {
                response.append(line);
            }
            reader.close();
            
            // 简单解析JSON获取access_token
            String responseStr = response.toString();
            int start = responseStr.indexOf("\"access_token\":\"") + 16;
            int end = responseStr.indexOf("\"", start);
            return responseStr.substring(start, end);
            
        } catch (Exception e) {
            System.err.println("获取access_token失败: " + e.getMessage());
            return null;
        }
    }
    
    private String performCloudOCR(File imageFile) {
        try {
            // 将图片转换为Base64
            byte[] imageBytes = Files.readAllBytes(imageFile.toPath());
            String base64Image = Base64.getEncoder().encodeToString(imageBytes);
            
            // 构建请求参数
            String params = "image=" + URLEncoder.encode(base64Image, "UTF-8") +
                           "&language_type=CHN_ENG"; // 中英文混合
            
            // 发送OCR请求
            String ocrUrl = OCR_URL + "?access_token=" + accessToken;
            URL url = new URL(ocrUrl);
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("POST");
            conn.setRequestProperty("Content-Type", "application/x-www-form-urlencoded");
            conn.setDoOutput(true);
            
            // 写入请求参数
            OutputStreamWriter writer = new OutputStreamWriter(conn.getOutputStream());
            writer.write(params);
            writer.flush();
            writer.close();
            
            // 读取响应
            BufferedReader reader = new BufferedReader(new InputStreamReader(conn.getInputStream()));
            String line;
            StringBuilder response = new StringBuilder();
            while ((line = reader.readLine()) != null) {
                response.append(line);
            }
            reader.close();
            
            // 解析OCR结果
            return parseOCRResponse(response.toString());
            
        } catch (Exception e) {
            System.err.println("云端OCR识别失败: " + e.getMessage());
            return "";
        }
    }
    
    private String parseOCRResponse(String jsonResponse) {
        // 简单解析百度OCR返回的JSON，提取words_result中的words
        StringBuilder result = new StringBuilder();
        
        try {
            String[] lines = jsonResponse.split("\"words\":\"");
            for (int i = 1; i < lines.length; i++) {
                int end = lines[i].indexOf("\"");
                if (end > 0) {
                    String text = lines[i].substring(0, end);
                    // 处理转义字符
                    text = text.replace("\\n", "\n").replace("\\t", " ");
                    result.append(text).append("\n");
                }
            }
        } catch (Exception e) {
            System.err.println("解析OCR响应失败: " + e.getMessage());
        }
        
        return result.toString().trim();
    }
    
    private List<BrandRankingEntry> parseRankingData(String ocrText, int imageIndex, String imageName, String category) {
        List<BrandRankingEntry> entries = new ArrayList<>();
        
        if (ocrText == null || ocrText.trim().isEmpty()) {
            return entries;
        }
        
        // 按行分割文本
        String[] lines = ocrText.split("\n");
        
        // 检测平台标题
        String currentPlatform = detectCurrentPlatform(ocrText);
        System.out.println("  检测到平台: " + currentPlatform);
        
        // 解析每一行数据
        for (String line : lines) {
            line = line.trim();
            if (line.isEmpty()) continue;
            
            // 检查是否是平台标题行
            String detectedPlatform = detectPlatformInLine(line);
            if (detectedPlatform != null) {
                currentPlatform = detectedPlatform;
                continue;
            }
            
            // 尝试解析品牌排行数据
            BrandRankingEntry entry = parseBrandLine(line, imageIndex, imageName, currentPlatform, category);
            if (entry != null) {
                entries.add(entry);
            }
        }
        
        return entries;
    }
    
    private String detectCurrentPlatform(String text) {
        String[] platforms = {"所选平台整体", "天猫", "淘宝", "京东", "抖音", "得物", "小红书"};
        
        for (String platform : platforms) {
            if (text.contains(platform)) {
                return platform;
            }
        }
        
        return "所选平台整体"; // 默认平台
    }
    
    private String detectPlatformInLine(String line) {
        String[] platforms = {"所选平台整体", "天猫", "淘宝", "京东", "抖音", "得物", "小红书"};
        
        for (String platform : platforms) {
            if (line.contains(platform)) {
                return platform;
            }
        }
        
        return null;
    }
    
    private BrandRankingEntry parseBrandLine(String line, int imageIndex, String imageName, String platform, String category) {
        // 正则表达式匹配品牌排行数据模式
        // 例如: "1 (0) apple/苹果 2.3亿 +24.3%"
        
        // 匹配排名
        Pattern rankPattern = Pattern.compile("^\\s*(\\d+)");
        Matcher rankMatcher = rankPattern.matcher(line);
        
        if (!rankMatcher.find()) {
            return null; // 不是排行数据行
        }
        
        String ranking = rankMatcher.group(1);
        
        // 匹配排名变化 (0), (↑3), (↓2), (+5), (-2)
        Pattern changePattern = Pattern.compile("\\(([0↑↓+-]\\d*)\\)");
        Matcher changeMatcher = changePattern.matcher(line);
        String rankingChange = changeMatcher.find() ? changeMatcher.group(1) : "";
        
        // 匹配品牌名 - 改进的正则表达式
        String brandName = "";
        Pattern brandPattern = Pattern.compile("\\d+\\s*\\([^)]*\\)\\s*([^\\d0-9]+?)\\s*[\\d\\.]+");
        Matcher brandMatcher = brandPattern.matcher(line);
        if (brandMatcher.find()) {
            brandName = brandMatcher.group(1).trim();
        } else {
            // 备用方案：手动提取品牌名
            String temp = line;
            temp = temp.replaceFirst("^\\s*\\d+\\s*\\([^)]*\\)\\s*", ""); // 移除排名部分
            temp = temp.replaceFirst("[\\d\\.]+[万亿千百元]+.*$", ""); // 移除销售额部分
            brandName = temp.trim();
        }
        
        // 匹配销售额
        Pattern salesPattern = Pattern.compile("([\\d\\.]+[万亿千百元]+)");
        Matcher salesMatcher = salesPattern.matcher(line);
        String salesAmount = salesMatcher.find() ? salesMatcher.group(1) : "";
        
        // 匹配增长率
        Pattern growthPattern = Pattern.compile("([+-]?[\\d\\.]+%)");
        Matcher growthMatcher = growthPattern.matcher(line);
        String growthRate = growthMatcher.find() ? growthMatcher.group(1) : "";
        
        // 只有当解析到有效数据时才创建对象
        if (!brandName.isEmpty() || !salesAmount.isEmpty()) {
            BrandRankingEntry entry = new BrandRankingEntry();
            entry.imageIndex = imageIndex;
            entry.imageName = imageName;
            entry.platform = platform;
            entry.ranking = ranking;
            entry.rankingChange = rankingChange;
            entry.brandName = brandName;
            entry.salesAmount = salesAmount;
            entry.growthRate = growthRate;
            entry.category = category;
            
            return entry;
        }
        
        return null;
    }
    
    private void generateCSVFile(List<BrandRankingEntry> entries, String fileName) {
        try {
            File csvFile = new File(fileName + "_cloud_brand_data.csv");
            PrintWriter writer = new PrintWriter(new FileWriter(csvFile));
            
            writer.println("图片序号,图片名称,平台,排名,排名变化,品牌名称,销售额,增长率,类别");
            
            for (BrandRankingEntry entry : entries) {
                writer.println(String.format("%d,%s,%s,%s,%s,%s,%s,%s,%s",
                    entry.imageIndex, entry.imageName, entry.platform, entry.ranking,
                    entry.rankingChange, entry.brandName, entry.salesAmount, entry.growthRate, entry.category));
            }
            
            writer.close();
            System.out.println("\n✅ 云端OCR解析CSV文件已生成: " + csvFile.getAbsolutePath());
        } catch (IOException e) {
            System.err.println("❌ 生成CSV文件失败: " + e.getMessage());
        }
    }
    
    // 品牌排行数据类
    static class BrandRankingEntry {
        int imageIndex;
        String imageName;
        String platform;
        String ranking;
        String rankingChange;
        String brandName;
        String salesAmount;
        String growthRate;
        String category;
    }
} 