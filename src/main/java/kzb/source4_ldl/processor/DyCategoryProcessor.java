package kzb.source4_ldl.processor;

import kzb.source4_ldl.util.SqlExecutor;

public class DyCategoryProcessor extends AbstractTableProcessor {
    
    public DyCategoryProcessor(SqlExecutor sqlExecutor) {
        super(sqlExecutor);
    }
    
    @Override
    public void process() {
        // 1. 删除表
        sqlExecutor.executeSql(
            "DROP TABLE IF EXISTS ods.dy_category_combine_category", 
            "删除抖音Category表"
        );
        
        // 2. 创建表
        String createSql = 
            "CREATE TABLE ods.dy_category_combine_category " +
            "ENGINE = MergeTree ORDER BY createtime AS " +
            "SELECT DY_category, SP_Category_I, SP_Category_II, SP_Category_III, createtime " +
            "FROM dim.dy_category_combine WHERE DY_category != '' order by  createtime DESC limit 1 by  DY_category";
        sqlExecutor.executeSql(createSql, "创建抖音Category表");
        
        // 3. 创建Join表
        tableOperator.createJoinTable(
            "ods.dy_category_combine_category", 
            "ods.dy_category_combine_category_dict", 
            "DY_category"
        );
        
        // 4. 验证数据
        dataValidator.validateTableCounts(
            "ods.dy_category_combine_category", 
            "ods.dy_category_combine_category_dict"
        );
    }
    
    @Override
    public String getProcessorName() {
        return "dy_category";
    }
    
    @Override
    public String getDescription() {
        return "抖音Category维度表处理";
    }
}