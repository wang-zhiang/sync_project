package kzb.source4_ldl.processor;

import kzb.source4_ldl.util.SqlExecutor;

public class DySubnameProcessor extends AbstractTableProcessor {
    
    public DySubnameProcessor(SqlExecutor sqlExecutor) {
        super(sqlExecutor);
    }
    
    @Override
    public void process() {
        // 1. 删除表
        sqlExecutor.executeSql(
            "DROP TABLE IF EXISTS ods.dy_category_combine_subname", 
            "删除抖音Subname表"
        );
        
        // 2. 创建表
        String createSql = 
            "CREATE TABLE ods.dy_category_combine_subname " +
            "ENGINE = MergeTree ORDER BY createtime AS " +
            "SELECT DY_subname, SP_Category_I, SP_Category_II, SP_Category_III, createtime " +
            "FROM dim.dy_category_combine WHERE DY_subname != '' order by  createtime DESC limit 1 by  DY_subname ";
        sqlExecutor.executeSql(createSql, "创建抖音Subname表");
        
        // 3. 创建Join表
        tableOperator.createJoinTable(
            "ods.dy_category_combine_subname", 
            "ods.dy_category_combine_subname_dict", 
            "DY_subname"
        );
        
        // 4. 验证数据
        dataValidator.validateTableCounts(
            "ods.dy_category_combine_subname", 
            "ods.dy_category_combine_subname_dict"
        );
    }
    
    @Override
    public String getProcessorName() {
        return "dy_subname";
    }
    
    @Override
    public String getDescription() {
        return "抖音Subname维度表处理";
    }
}