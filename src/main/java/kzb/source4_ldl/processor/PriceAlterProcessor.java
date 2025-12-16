package kzb.source4_ldl.processor;

import kzb.source4_ldl.util.SqlExecutor;

public class PriceAlterProcessor extends AbstractTableProcessor {
    
    public PriceAlterProcessor(SqlExecutor sqlExecutor) {
        super(sqlExecutor);
    }
    
    @Override
    public void process() {
        // 1. 删除临时表
        sqlExecutor.executeSql(
            "DROP TABLE IF EXISTS tmp.source4_ldl_price_alter", 
            "删除价格维度临时表"
        );
        
        // 2. 创建临时表
        String createTempSql = 
            "CREATE TABLE tmp.source4_ldl_price_alter " +
            "ENGINE = MergeTree ORDER BY createtime AS " +
            "SELECT * FROM dim.source4_ldl_price_alter " +
            "ORDER BY createtime DESC LIMIT 1 BY Channel,Itemid";
        sqlExecutor.executeSql(createTempSql, "创建价格维度临时表");
        
        // 3. 删除字典表和辅助表
        sqlExecutor.executeSql(
            "DROP TABLE IF EXISTS ods.alter_price_sl_dict", 
            "删除价格维度字典表"
        );
        sqlExecutor.executeSql(
            "DROP TABLE IF EXISTS ods.alter_price_sl_dict_assist", 
            "删除价格维度辅助表"
        );
        
        // 4. 创建Join表和辅助表
        tableOperator.createJoinTable(
            "tmp.source4_ldl_price_alter", 
            "ods.alter_price_sl_dict", 
            "Channel,Itemid"
        );
        
        tableOperator.createAssistTable(
            "tmp.source4_ldl_price_alter", 
            "ods.alter_price_sl_dict_assist"
        );
        
        // 5. 验证三个表数据
        dataValidator.validateThreeTableCounts(
            "tmp.source4_ldl_price_alter", 
            "ods.alter_price_sl_dict", 
            "ods.alter_price_sl_dict_assist"
        );
    }
    
    @Override
    public String getProcessorName() {
        return "price_alter";
    }
    
    @Override
    public String getDescription() {
        return "价格维度表处理";
    }
}