package kzb.source4_ldl.processor;

import kzb.source4_ldl.util.SqlExecutor;

public class ShopBrandTableProcessor extends AbstractTableProcessor {
    
    public ShopBrandTableProcessor(SqlExecutor sqlExecutor) {
        super(sqlExecutor);
    }
    
    @Override
    public void process() {
        // 1. 删除 test.shop_brand
        sqlExecutor.executeSql(
            "DROP TABLE IF EXISTS test.shop_brand", 
            "删除店铺品牌表"
        );
        
        // 2. 创建 test.shop_brand
        String createShopBrandSql = 
            "CREATE TABLE test.shop_brand " +
            "ENGINE = MergeTree ORDER BY createtime AS " +
            "SELECT Shop, SP_Brand_CNEN, SP_Brand_CN, SP_Brand_EN, createtime " +
            "FROM dim.shop_brand " +
            "ORDER BY createtime DESC LIMIT 1 BY Shop";
        sqlExecutor.executeSql(createShopBrandSql, "创建店铺品牌MergeTree表");
        
        // 3. 创建Join表
        tableOperator.createJoinTable(
            "test.shop_brand", 
            "ods.shop_brand_dict", 
            "Shop"
        );
        
        // 4. 验证数据
        dataValidator.validateTableCounts(
            "test.shop_brand", 
            "ods.shop_brand_dict"
        );
    }
    
    @Override
    public String getProcessorName() {
        return "shop_brand";
    }
    
    @Override
    public String getDescription() {
        return "店铺品牌维度表处理";
    }
}