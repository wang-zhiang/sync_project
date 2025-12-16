package from_csv.aindustry_brand;

import org.apache.poi.hssf.usermodel.*;
import org.apache.poi.ss.usermodel.*;
import org.apache.poi.xssf.usermodel.*;

import java.io.*;
import java.util.*;

/**
 * Excel图片检测器
 * 功能：遍历品牌洞察文件夹，检测所有Excel文件中是否包含图片
 */
public class ExcelImageDetector {
    
    private static final int MIN_IMAGE_SIZE = 1000; // 最小图片大小（字节），过滤小图标
    
    public static void main(String[] args) {
        try {
            ExcelImageDetector detector = new ExcelImageDetector();
            
            // 设置基础路径
            String basePath = "D:\\wzza\\develop\\idea_project\\ceshi\\src\\main\\java\\from_csv\\aindustry_brand\\brand";
            
            detector.detectImagesInBrandFolders(basePath);
            
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
    /**
     * 检测指定路径下所有Excel文件是否包含图片（递归遍历）
     * @param basePath 基础路径
     */
    public void detectImagesInBrandFolders(String basePath) throws IOException {
        System.out.println("开始检测路径下的Excel图片...");
        System.out.println("基础路径: " + basePath);
        System.out.println(repeatString("=", 80));
        
        File baseDir = new File(basePath);
        if (!baseDir.exists()) {
            System.out.println("❌ 基础路径不存在: " + basePath);
            return;
        }
        
        int totalFilesChecked = 0;
        int totalFilesWithImages = 0;
        
        // 递归遍历所有文件夹和Excel文件
        List<File> allExcelFiles = findAllExcelFiles(baseDir);
        
        if (allExcelFiles.isEmpty()) {
            System.out.println("❌ 没有找到任何Excel文件");
            return;
        }
        
        System.out.println("找到 " + allExcelFiles.size() + " 个Excel文件");
        
        // 按路径排序
        allExcelFiles.sort(Comparator.comparing(File::getAbsolutePath));
        
        String currentFolder = "";
        
        // 检测每个Excel文件
        for (File excelFile : allExcelFiles) {
            totalFilesChecked++;
            
            // 获取相对路径用于显示
            String relativePath = getRelativePath(baseDir, excelFile.getParentFile());
            
            // 如果是新文件夹，输出文件夹信息
            if (!relativePath.equals(currentFolder)) {
                currentFolder = relativePath;
                System.out.println("\n📁 " + (relativePath.isEmpty() ? "根目录" : relativePath));
            }
            
            boolean hasImages = detectImagesInExcel(excelFile);
            
            if (hasImages) {
                totalFilesWithImages++;
                System.out.println("  🖼️  " + excelFile.getName() + " - 包含图片");
            }
            // 无图片的文件不输出
        }
        
        // 输出总结
        System.out.println("\n" + repeatString("=", 80));
        System.out.println("检测完成！");
        System.out.println("总检测文件数: " + totalFilesChecked);
        System.out.println("包含图片的文件数: " + totalFilesWithImages);
        System.out.println(repeatString("=", 80));
    }
    
    /**
     * 检测单个Excel文件是否包含图片
     * @param excelFile Excel文件
     * @return true表示包含图片，false表示不包含图片
     */
    private boolean detectImagesInExcel(File excelFile) {
        try {
            if (excelFile.getName().toLowerCase().endsWith(".xls")) {
                return detectImagesInXLS(excelFile);
            } else if (excelFile.getName().toLowerCase().endsWith(".xlsx")) {
                return detectImagesInXLSX(excelFile);
            }
        } catch (Exception e) {
            System.out.println("    ❌ 检测失败: " + excelFile.getName() + " - " + e.getMessage());
        }
        return false;
    }
    
    /**
     * 检测XLS文件是否包含图片
     */
    private boolean detectImagesInXLS(File excelFile) throws IOException {
        try (FileInputStream fis = new FileInputStream(excelFile);
             HSSFWorkbook workbook = new HSSFWorkbook(fis)) {
            
            // 遍历所有工作表
            for (int sheetIndex = 0; sheetIndex < workbook.getNumberOfSheets(); sheetIndex++) {
                HSSFSheet sheet = workbook.getSheetAt(sheetIndex);
                
                // 获取绘图对象集合
                HSSFPatriarch patriarch = sheet.getDrawingPatriarch();
                if (patriarch == null) {
                    continue;
                }
                
                // 获取所有形状对象
                List<HSSFShape> shapes = patriarch.getChildren();
                
                for (HSSFShape shape : shapes) {
                    if (shape instanceof HSSFPicture) {
                        HSSFPicture picture = (HSSFPicture) shape;
                        
                        try {
                            // 提取图片数据
                            HSSFPictureData pictureData = picture.getPictureData();
                            byte[] imageBytes = pictureData.getData();
                            
                            // 过滤太小的图片（可能是图标）
                            if (imageBytes.length >= MIN_IMAGE_SIZE) {
                                return true; // 找到有效图片
                            }
                        } catch (Exception e) {
                            // 忽略单个图片的错误，继续检测其他图片
                        }
                    }
                }
            }
        }
        return false;
    }
    
    /**
     * 检测XLSX文件是否包含图片
     */
    private boolean detectImagesInXLSX(File excelFile) throws IOException {
        try (FileInputStream fis = new FileInputStream(excelFile);
             XSSFWorkbook workbook = new XSSFWorkbook(fis)) {
            
            // 遍历所有工作表
            for (int sheetIndex = 0; sheetIndex < workbook.getNumberOfSheets(); sheetIndex++) {
                XSSFSheet sheet = workbook.getSheetAt(sheetIndex);
                
                // 获取绘图对象集合
                XSSFDrawing drawing = sheet.getDrawingPatriarch();
                if (drawing == null) {
                    continue;
                }
                
                // 获取所有形状对象
                List<XSSFShape> shapes = drawing.getShapes();
                
                for (XSSFShape shape : shapes) {
                    if (shape instanceof XSSFPicture) {
                        XSSFPicture picture = (XSSFPicture) shape;
                        
                        try {
                            // 提取图片数据
                            XSSFPictureData pictureData = picture.getPictureData();
                            byte[] imageBytes = pictureData.getData();
                            
                            // 过滤太小的图片（可能是图标）
                            if (imageBytes.length >= MIN_IMAGE_SIZE) {
                                return true; // 找到有效图片
                            }
                        } catch (Exception e) {
                            // 忽略单个图片的错误，继续检测其他图片
                        }
                    }
                }
            }
        }
        return false;
    }
    
    /**
     * 递归查找所有Excel文件
     */
    private List<File> findAllExcelFiles(File directory) {
        List<File> excelFiles = new ArrayList<>();
        
        if (!directory.exists() || !directory.isDirectory()) {
            return excelFiles;
        }
        
        File[] files = directory.listFiles();
        if (files == null) {
            return excelFiles;
        }
        
        for (File file : files) {
            if (file.isDirectory()) {
                // 递归查找子文件夹
                excelFiles.addAll(findAllExcelFiles(file));
            } else if (file.isFile()) {
                String fileName = file.getName().toLowerCase();
                if (fileName.endsWith(".xls") || fileName.endsWith(".xlsx")) {
                    excelFiles.add(file);
                }
            }
        }
        
        return excelFiles;
    }
    
    /**
     * 获取相对路径
     */
    private String getRelativePath(File baseDir, File targetDir) {
        try {
            String basePath = baseDir.getCanonicalPath();
            String targetPath = targetDir.getCanonicalPath();
            
            if (targetPath.equals(basePath)) {
                return "";
            }
            
            if (targetPath.startsWith(basePath)) {
                String relativePath = targetPath.substring(basePath.length());
                if (relativePath.startsWith(File.separator)) {
                    relativePath = relativePath.substring(1);
                }
                return relativePath.replace(File.separator, "/");
            }
            
            return targetPath;
        } catch (IOException e) {
            return targetDir.getName();
        }
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
     * 详细检测模式 - 输出每个文件的详细信息
     * @param basePath 基础路径
     */
    public void detectImagesInBrandFoldersDetailed(String basePath) throws IOException {
        System.out.println("开始详细检测路径下的Excel图片...");
        System.out.println("基础路径: " + basePath);
        System.out.println(repeatString("=", 80));
        
        File baseDir = new File(basePath);
        if (!baseDir.exists()) {
            System.out.println("❌ 基础路径不存在: " + basePath);
            return;
        }
        
        // 递归查找所有Excel文件
        List<File> allExcelFiles = findAllExcelFiles(baseDir);
        
        if (allExcelFiles.isEmpty()) {
            System.out.println("❌ 没有找到任何Excel文件");
            return;
        }
        
        System.out.println("找到 " + allExcelFiles.size() + " 个Excel文件");
        
        // 按路径排序
        allExcelFiles.sort(Comparator.comparing(File::getAbsolutePath));
        
        // 存储结果
        List<String> filesWithImages = new ArrayList<>();
        
        String currentFolder = "";
        
        // 检测每个Excel文件
        for (File excelFile : allExcelFiles) {
            // 获取相对路径用于显示
            String relativePath = getRelativePath(baseDir, excelFile.getParentFile());
            
            // 如果是新文件夹，输出文件夹信息
            if (!relativePath.equals(currentFolder)) {
                currentFolder = relativePath;
                System.out.println("\n📁 " + (relativePath.isEmpty() ? "根目录" : relativePath));
            }
            
            boolean hasImages = detectImagesInExcel(excelFile);
            String fullRelativePath = relativePath.isEmpty() ? 
                excelFile.getName() : 
                relativePath + "/" + excelFile.getName();
            
            if (hasImages) {
                filesWithImages.add(fullRelativePath);
                System.out.println("  🖼️  " + excelFile.getName() + " - 包含图片");
            }
            // 无图片的文件不输出
        }
        
        // 输出汇总结果
        System.out.println("\n" + repeatString("=", 80));
        System.out.println("📋 包含图片的文件列表:");
        if (filesWithImages.isEmpty()) {
            System.out.println("  (所有文件都不包含图片)");
        } else {
            for (String file : filesWithImages) {
                System.out.println("  🖼️  " + file);
            }
        }
        
        // 输出统计
        System.out.println("\n" + repeatString("=", 80));
        System.out.println("检测完成！");
        System.out.println("包含图片的文件数: " + filesWithImages.size());
        System.out.println(repeatString("=", 80));
    }
} 