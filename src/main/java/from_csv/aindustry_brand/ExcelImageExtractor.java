package from_csv.aindustry_brand;
import org.apache.poi.hssf.usermodel.*;
import org.apache.poi.ss.usermodel.*;
import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.io.*;
import java.util.*;

/**
 * Excel图片提取器
 * 功能：从XLS文件中提取所有图片，无论图片位置是否规整
 */
public class ExcelImageExtractor {
    
    private static final int MIN_IMAGE_SIZE = 1000; // 最小图片大小（字节），过滤小图标
    
    public static void main(String[] args) {
        try {
            ExcelImageExtractor extractor = new ExcelImageExtractor();
            
            // 测试文件路径
            String excelPath = "D:\\wzza\\develop\\idea_project\\ceshi\\src\\main\\java\\from_csv\\aindustry_brand\\brand\\2025-1品牌榜单\\3c数码\\3c数码配件.xls";
            String outputDir = "extracted_images";
            
            extractor.extractImagesFromExcel(excelPath, outputDir);
            
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
    /**
     * 从Excel文件提取所有图片
     * @param excelFilePath Excel文件路径
     * @param outputDirectory 输出目录
     */
    public void extractImagesFromExcel(String excelFilePath, String outputDirectory) throws IOException {
        System.out.println("开始提取Excel文件中的图片: " + excelFilePath);
        
        // 创建输出目录
        File outputDir = new File(outputDirectory);
        if (!outputDir.exists()) {
            outputDir.mkdirs();
            System.out.println("创建输出目录: " + outputDir.getAbsolutePath());
        }
        
        // 读取Excel文件
        FileInputStream fis = new FileInputStream(excelFilePath);
        HSSFWorkbook workbook = new HSSFWorkbook(fis);
        
        int totalImageCount = 0;
        String baseFileName = new File(excelFilePath).getName().replace(".xls", "");
        
        // 遍历所有工作表
        for (int sheetIndex = 0; sheetIndex < workbook.getNumberOfSheets(); sheetIndex++) {
            HSSFSheet sheet = workbook.getSheetAt(sheetIndex);
            String sheetName = sheet.getSheetName();
            
            System.out.println("\n处理工作表: " + sheetName);
            
            // 获取绘图对象集合
            HSSFPatriarch patriarch = sheet.getDrawingPatriarch();
            if (patriarch == null) {
                System.out.println("  工作表中没有绘图对象");
                continue;
            }
            
            // 获取所有形状对象
            List<HSSFShape> shapes = patriarch.getChildren();
            int sheetImageCount = 0;
            
            for (HSSFShape shape : shapes) {
                if (shape instanceof HSSFPicture) {
                    HSSFPicture picture = (HSSFPicture) shape;
                    
                    try {
                        // 提取图片数据
                        HSSFPictureData pictureData = picture.getPictureData();
                        byte[] imageBytes = pictureData.getData();
                        
                        // 过滤太小的图片（可能是图标）
                        if (imageBytes.length < MIN_IMAGE_SIZE) {
                            System.out.println("  跳过小图片 (大小: " + imageBytes.length + " bytes)");
                            continue;
                        }
                        
                        // 确定图片格式
                        String imageFormat = getImageFormat(pictureData);
                        String fileExtension = getFileExtension(imageFormat);
                        
                        // 生成文件名
                        totalImageCount++;
                        sheetImageCount++;
                        String fileName = String.format("%s_sheet%d_image%d.%s", 
                            baseFileName, sheetIndex + 1, sheetImageCount, fileExtension);
                        
                        // 保存图片
                        File imageFile = new File(outputDir, fileName);
                        saveImageBytes(imageBytes, imageFile);
                        
                        // 获取图片信息
                        String imageInfo = getImageInfo(imageBytes);
                        System.out.println(String.format("  ✅ 提取图片: %s (%s, 大小: %.1f KB)", 
                            fileName, imageInfo, imageBytes.length / 1024.0));
                        
                    } catch (Exception e) {
                        System.err.println("  ❌ 提取图片失败: " + e.getMessage());
                    }
                }
            }
            
            if (sheetImageCount == 0) {
                System.out.println("  工作表中没有找到图片");
            }
        }
        
        // 关闭资源
        workbook.close();
        fis.close();
        
        // 输出统计信息
        System.out.println("\n" + repeatString("=", 60));
        System.out.println("图片提取完成！");
        System.out.println("总提取图片数: " + totalImageCount);
        System.out.println("输出目录: " + outputDir.getAbsolutePath());
        System.out.println(repeatString("=", 60));
    }
    
    /**
     * 获取图片格式
     */
    private String getImageFormat(HSSFPictureData pictureData) {
        switch (pictureData.getPictureType()) {
            case HSSFWorkbook.PICTURE_TYPE_JPEG:
                return "JPEG";
            case HSSFWorkbook.PICTURE_TYPE_PNG:
                return "PNG";
            case HSSFWorkbook.PICTURE_TYPE_DIB:
                return "BMP";
            case HSSFWorkbook.PICTURE_TYPE_WMF:
                return "WMF";
            case HSSFWorkbook.PICTURE_TYPE_EMF:
                return "EMF";
            default:
                return "UNKNOWN";
        }
    }
    
    /**
     * 根据图片格式获取文件扩展名
     */
    private String getFileExtension(String imageFormat) {
        switch (imageFormat.toUpperCase()) {
            case "JPEG":
                return "jpg";
            case "PNG":
                return "png";
            case "BMP":
            case "DIB":
                return "bmp";
            case "WMF":
                return "wmf";
            case "EMF":
                return "emf";
            default:
                return "img";
        }
    }
    
    /**
     * 保存图片字节数组到文件
     */
    private void saveImageBytes(byte[] imageBytes, File outputFile) throws IOException {
        try (FileOutputStream fos = new FileOutputStream(outputFile)) {
            fos.write(imageBytes);
            fos.flush();
        }
    }
    
    /**
     * 获取图片信息（尺寸等）
     */
    private String getImageInfo(byte[] imageBytes) {
        try {
            BufferedImage image = ImageIO.read(new ByteArrayInputStream(imageBytes));
            if (image != null) {
                return String.format("%d×%d", image.getWidth(), image.getHeight());
            }
        } catch (Exception e) {
            // 忽略错误，返回默认信息
        }
        return "未知尺寸";
    }
    
    /**
     * 重复字符串（兼容Java 8）
     */
    private String repeatString(String str, int count) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < count; i++) {
            sb.append(str);
        }
        return sb.toString();
    }
    
    /**
     * 批量处理多个Excel文件
     */
    public void batchExtractImages(String inputDirectory, String outputDirectory) throws IOException {
        File inputDir = new File(inputDirectory);
        if (!inputDir.exists() || !inputDir.isDirectory()) {
            throw new IllegalArgumentException("输入目录不存在: " + inputDirectory);
        }
        
        File[] excelFiles = inputDir.listFiles((dir, name) -> 
            name.toLowerCase().endsWith(".xls") || name.toLowerCase().endsWith(".xlsx"));
        
        if (excelFiles == null || excelFiles.length == 0) {
            System.out.println("没有找到Excel文件");
            return;
        }
        
        System.out.println("找到 " + excelFiles.length + " 个Excel文件");
        
        for (File excelFile : excelFiles) {
            System.out.println("\n" + repeatString("=", 80));
            System.out.println("处理文件: " + excelFile.getName());
            
            String fileOutputDir = outputDirectory + "/" + excelFile.getName().replace(".xls", "").replace(".xlsx", "");
            
            try {
                extractImagesFromExcel(excelFile.getAbsolutePath(), fileOutputDir);
            } catch (Exception e) {
                System.err.println("处理文件失败: " + excelFile.getName() + " - " + e.getMessage());
            }
        }
    }
} 