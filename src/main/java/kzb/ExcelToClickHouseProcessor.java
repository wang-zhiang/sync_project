package kzb;

public class ExcelToClickHouseProcessor {

    //要注意可能已经跑完了，但是后台还在执行，此时还是不能加索引
    public static void main(String[] args) {
        String excelFilePath = "D:\\wzza\\develop\\idea_project\\ceshi\\src\\main\\java\\kzb\\ceshi.xlsx";
        String[] targetNodes = {
                "hadoop104", "hadoop105", "hadoop106", "hadoop107",
                "hadoop108", "hadoop109", "hadoop110"
        };
        // 初始化CK连接配置
        ClickHouseConfig ckConfig = new ClickHouseConfig(targetNodes, "default", "smartpath");
        // 处理Excel
        try (ExcelReader reader = new ExcelReader(excelFilePath)) {
//            SheetHandler handler = new ProcessSheetHandler(ckConfig);
//            handler.handleSheet(reader.getSheet("处理"));

//              SheetHandler handler = new BrandSheetHandler(ckConfig);
//              handler.handleSheet(reader.getSheet("Brand"));


             SheetHandler handler = new CategoryTaoxiSheetHandler(ckConfig);
             handler.handleSheet(reader.getSheet("Category-淘系"));

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}