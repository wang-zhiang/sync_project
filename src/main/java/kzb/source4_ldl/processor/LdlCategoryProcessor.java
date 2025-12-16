package kzb.source4_ldl.processor;

import kzb.source4_ldl.util.SqlExecutor;

public class LdlCategoryProcessor extends AbstractTableProcessor {
    
    public LdlCategoryProcessor(SqlExecutor sqlExecutor) {
        super(sqlExecutor);
    }
    
    @Override
    public void process() {
        // 1. 删除表
        sqlExecutor.executeSql(
            "DROP TABLE IF EXISTS test.ldl_category_combine", 
            "删除LDL分类表"
        );
        
        // 2. 创建表
        String createSql = 
            "CREATE TABLE test.ldl_category_combine " +
            "ENGINE = MergeTree ORDER BY createtime AS " +
            "SELECT LDL_categoryname2, SP_Category_I, SP_Category_II, SP_Category_III, createtime " +
            "FROM dim.ldl_category_combine " +
            "ORDER BY createtime DESC LIMIT 1 BY LDL_categoryname2";
        sqlExecutor.executeSql(createSql, "创建LDL分类表");
        
        // 3. 创建Join表
        tableOperator.createJoinTable(
            "test.ldl_category_combine", 
            "ods.ldl_category_combine_dict", 
            "LDL_categoryname2"
        );
        
        // 4. 验证数据
        dataValidator.validateTableCounts(
            "test.ldl_category_combine", 
            "ods.ldl_category_combine_dict"
        );
    }
    
    @Override
    public String getProcessorName() {
        return "ldl_category";
    }
    
    @Override
    public String getDescription() {
        return "LDL分类维度表处理";
    }
}