package kzb.source4_ldl.processor;

import kzb.source4_ldl.util.SqlExecutor;

public class ItemIdBrandTableProcessor extends AbstractTableProcessor {
    
    public ItemIdBrandTableProcessor(SqlExecutor sqlExecutor) {
        super(sqlExecutor);
    }
    
    @Override
    public void process() {
        // 1. 删除 test.itemid_brand_combine
        sqlExecutor.executeSql(
            "DROP TABLE IF EXISTS test.itemid_brand_combine", 
            "删除ItemId品牌表"
        );
        
        // 2. 创建 test.itemid_brand_combine
        String createItemIdBrandSql = 
            "CREATE TABLE test.itemid_brand_combine " +
            "ENGINE = MergeTree ORDER BY createtime AS " +
            "SELECT * FROM dim.itemid_brand_combine " +
            "ORDER BY createtime DESC LIMIT 1 BY Channel, Itemid";
        sqlExecutor.executeSql(createItemIdBrandSql, "创建ItemId品牌MergeTree表");
        
        // 3. 删除字典表和辅助表
        sqlExecutor.executeSql(
            "DROP TABLE IF EXISTS ods.itemid_brand_combine_dict", 
            "删除ItemId品牌字典表"
        );
        sqlExecutor.executeSql(
            "DROP TABLE IF EXISTS ods.itemid_brand_combine_dict_assist", 
            "删除ItemId品牌辅助表"
        );
        
        // 4. 创建Join表 (多个关联键)
        tableOperator.createJoinTable(
            "test.itemid_brand_combine", 
            "ods.itemid_brand_combine_dict", 
            "Channel, Itemid"
        );
        
        // 5. 创建辅助表 (MergeTree引擎)
        tableOperator.createAssistTable(
            "test.itemid_brand_combine", 
            "ods.itemid_brand_combine_dict_assist"
        );
        
        // 6. 验证三个表数据
        dataValidator.validateThreeTableCounts(
            "test.itemid_brand_combine", 
            "ods.itemid_brand_combine_dict", 
            "ods.itemid_brand_combine_dict_assist"
        );
    }
    
    @Override
    public String getProcessorName() {
        return "itemid_brand";
    }
    
    @Override
    public String getDescription() {
        return "品牌ItemId处理";
    }
}