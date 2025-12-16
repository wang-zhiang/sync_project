package kzb.source4_ldl.processor;

import kzb.source4_ldl.util.SqlExecutor;

public class S4CategoryProcessor extends AbstractTableProcessor {
    
    public S4CategoryProcessor(SqlExecutor sqlExecutor) {
        super(sqlExecutor);
    }
    
    @Override
    public void process() {
        // 1. 删除临时表
        sqlExecutor.executeSql(
            "DROP TABLE IF EXISTS tmp.s4_category_combine", 
            "删除S4分类临时表"
        );
        
        // 2. 创建临时表
        String createTempSql = 
            "CREATE TABLE tmp.s4_category_combine " +
            "ENGINE = MergeTree ORDER BY createtime AS " +
            "SELECT * FROM dim.s4_category_combine " +
            "ORDER BY createtime DESC LIMIT 1 BY Channel,S4_rootcategoryname,S4_categoryname,S4_subcategoryname";
        sqlExecutor.executeSql(createTempSql, "创建S4分类临时表");
        
        // 3. 删除字典表和辅助表
        sqlExecutor.executeSql(
            "DROP TABLE IF EXISTS ods.s4_category_combine_dict", 
            "删除S4分类字典表"
        );
        sqlExecutor.executeSql(
            "DROP TABLE IF EXISTS ods.s4_category_combine_dict_assist", 
            "删除S4分类辅助表"
        );
        
        // 4. 创建Join表和辅助表
        tableOperator.createJoinTable(
            "tmp.s4_category_combine", 
            "ods.s4_category_combine_dict", 
            "Channel,S4_rootcategoryname,S4_categoryname,S4_subcategoryname"
        );
        
        tableOperator.createAssistTable(
            "tmp.s4_category_combine", 
            "ods.s4_category_combine_dict_assist"
        );
        
        // 5. 验证三个表数据
        dataValidator.validateThreeTableCounts(
            "tmp.s4_category_combine", 
            "ods.s4_category_combine_dict", 
            "ods.s4_category_combine_dict_assist"
        );
    }
    
    @Override
    public String getProcessorName() {
        return "s4_category";
    }
    
    @Override
    public String getDescription() {
        return "S4分类维度表处理";
    }
}