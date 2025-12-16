package kzb.source4_ldl.processor;

import kzb.source4_ldl.util.SqlExecutor;

public class BrandTableProcessor extends AbstractTableProcessor {
    
    public BrandTableProcessor(SqlExecutor sqlExecutor) {
        super(sqlExecutor);
    }
    
    @Override
    public void process() {
        // 1. 删除 ods.brandlist_all_new
        sqlExecutor.executeSql(
            "DROP TABLE IF EXISTS ods.brandlist_all_new", 
            "删除品牌表"
        );
        
        // 2. 创建 ods.brandlist_all_new
        String createBrandSql = 
            "CREATE TABLE ods.brandlist_all_new " +
            "ENGINE = MergeTree ORDER BY Brandname AS " +
            "SELECT lower(replaceRegexpAll(Brandname, '[ \\n\\t\\r]+', '')) Brandname, " +
            "SP_Brand_CNEN, SP_Brand_CN, SP_Brand_EN, createtime " +
            "FROM dim.brandlist_all_new " +
            "ORDER BY createtime DESC LIMIT 1 BY Brandname";
        sqlExecutor.executeSql(createBrandSql, "创建品牌MergeTree表");
        
        // 3. 创建Join表
        tableOperator.createJoinTable(
            "ods.brandlist_all_new", 
            "ods.brandname_all_channel_1206_dict", 
            "Brandname"
        );
        
        // 4. 验证数据
        dataValidator.validateTableCounts(
            "ods.brandlist_all_new", 
            "ods.brandname_all_channel_1206_dict"
        );
    }
    
    @Override
    public String getProcessorName() {
        return "brand";
    }
    
    @Override
    public String getDescription() {
        return "品牌维度表处理";
    }
}