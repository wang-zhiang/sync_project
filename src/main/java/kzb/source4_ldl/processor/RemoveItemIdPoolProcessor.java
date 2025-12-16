package kzb.source4_ldl.processor;

import kzb.source4_ldl.util.SqlExecutor;

public class RemoveItemIdPoolProcessor extends AbstractTableProcessor {
    
    public RemoveItemIdPoolProcessor(SqlExecutor sqlExecutor) {
        super(sqlExecutor);
    }
    
    @Override
    public void process() {
        // 1. 删除临时表
        sqlExecutor.executeSql(
            "DROP TABLE IF EXISTS tmp.remove_itemid_pool", 
            "删除下架商品池临时表"
        );
        
        // 2. 创建临时表
        String createTempSql = 
            "CREATE TABLE tmp.remove_itemid_pool " +
            "ENGINE = MergeTree ORDER BY createtime AS " +
            "SELECT YM, Channel, Itemid, SP_Category_I, SP_Category_II, SP_Category_III, createtime " +
            "FROM dim.remove_itemid_pool " +
            "ORDER BY createtime DESC LIMIT 1 BY Channel,Itemid";
        sqlExecutor.executeSql(createTempSql, "创建下架商品池临时表");
        
        // 3. 删除字典表和辅助表
        sqlExecutor.executeSql(
            "DROP TABLE IF EXISTS ods.remove_itemid_pool_dict", 
            "删除下架商品池字典表"
        );
        sqlExecutor.executeSql(
            "DROP TABLE IF EXISTS ods.remove_itemid_pool_dict_assist", 
            "删除下架商品池辅助表"
        );
        
        // 4. 创建Join表和辅助表
        tableOperator.createJoinTable(
            "tmp.remove_itemid_pool", 
            "ods.remove_itemid_pool_dict", 
            "Channel,Itemid"
        );
        
        tableOperator.createAssistTable(
            "tmp.remove_itemid_pool", 
            "ods.remove_itemid_pool_dict_assist"
        );
        
        // 5. 验证三个表数据
        dataValidator.validateThreeTableCounts(
            "tmp.remove_itemid_pool", 
            "ods.remove_itemid_pool_dict", 
            "ods.remove_itemid_pool_dict_assist"
        );
    }
    
    @Override
    public String getProcessorName() {
        return "remove_itemid_pool";
    }
    
    @Override
    public String getDescription() {
        return "下架商品池维度表处理";
    }
}