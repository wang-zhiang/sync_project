package kzb.source4_ldl;

import kzb.ClickHouseConfig;
import kzb.source4_ldl.processor.*;
import kzb.source4_ldl.util.DataValidator;
import kzb.source4_ldl.util.SqlExecutor;
import kzb.source4_ldl.util.TableOperator;
import java.util.*;

public class ClickHouseAutoProcessor {
    private ClickHouseConfig config;
    private SqlExecutor sqlExecutor;
    private TableOperator tableOperator;
    private DataValidator dataValidator;
    private Map<String, TableProcessor> processors;
    
    public ClickHouseAutoProcessor() {
        // 初始化配置
        this.config = new ClickHouseConfig(
            new String[]{"192.168.5.111"}, 
            "default", 
            "smartpath"
        );
        this.sqlExecutor = new SqlExecutor(config.getConnectionUrl("192.168.5.111"));
        this.tableOperator = new TableOperator(sqlExecutor);
        this.dataValidator = new DataValidator(sqlExecutor);
        
        // 注册所有处理器
        initProcessors();
    }
    
    public ClickHouseAutoProcessor(ClickHouseConfig config) {
        this.config = config;
        this.sqlExecutor = new SqlExecutor(config.getConnectionUrl("192.168.5.111"));
        this.tableOperator = new TableOperator(sqlExecutor);
        this.dataValidator = new DataValidator(sqlExecutor);
        
        // 注册所有处理器
        initProcessors();
    }
    
    private void initProcessors() {
        processors = new HashMap<>();
        
        // 现有处理器
        processors.put("brand", new BrandTableProcessor(sqlExecutor));
        processors.put("shop_brand", new ShopBrandTableProcessor(sqlExecutor));
        processors.put("itemid_brand", new ItemIdBrandTableProcessor(sqlExecutor));
        
        // 新增处理器
        processors.put("s4_category", new S4CategoryProcessor(sqlExecutor));
        processors.put("ldl_category", new LdlCategoryProcessor(sqlExecutor));
        processors.put("dy_category", new DyCategoryProcessor(sqlExecutor));
        processors.put("dy_subname", new DySubnameProcessor(sqlExecutor));
        processors.put("remove_itemid_pool", new RemoveItemIdPoolProcessor(sqlExecutor));
        processors.put("price_alter", new PriceAlterProcessor(sqlExecutor));
        processors.put("category_itemid", new CategoryItemIdProcessor(sqlExecutor));
    }
    
    // 执行指定的处理器
    public void execute(String... processorNames) {
        if (processorNames.length == 0) {
            executeAll();
            return;
        }
        
        for (String name : processorNames) {
            if (processors.containsKey(name)) {
                System.out.println("=== 开始处理: " + name + " ===");
                try {
                    processors.get(name).process();
                    System.out.println("=== 完成处理: " + name + " ===");
                } catch (Exception e) {
                    System.err.println("=== 处理失败: " + name + ", 错误: " + e.getMessage() + " ===");
                    e.printStackTrace();
                }
            } else {
                System.err.println("未找到处理器: " + name);
                System.out.println("可用的处理器: " + getAvailableProcessors());
            }
        }
    }
    
    // 执行所有处理器
    public void executeAll() {
        execute("brand", "shop_brand", "itemid_brand", "s4_category", "ldl_category", 
                "dy_category", "dy_subname", "remove_itemid_pool", "price_alter", "category_itemid");
    }
    
    // 获取所有可用的处理器名称
    public Set<String> getAvailableProcessors() {
        return processors.keySet();
    }
    
    // 测试方法，可以直接在IDE中运行
    public static void main(String[] args) {
        ClickHouseAutoProcessor processor = new ClickHouseAutoProcessor();
        
        System.out.println("ClickHouse自动化SQL执行器");
        System.out.println("可用的处理器: " + processor.getAvailableProcessors());
        
        // 可以在这里选择执行哪些处理器
        // processor.execute("brand");  // 只执行品牌处理
        // processor.execute("brand", "shop_brand");  // 执行品牌和店铺品牌处理
      //  processor.execute("dy_subname","dy_category");  // 只执行category itemid处理
         processor.executeAll();  // 执行所有处理器
    }
}