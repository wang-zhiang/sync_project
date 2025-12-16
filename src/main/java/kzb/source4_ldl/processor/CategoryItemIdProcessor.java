package kzb.source4_ldl.processor;

import kzb.source4_ldl.util.DataValidator;
import kzb.source4_ldl.util.SqlExecutor;
import kzb.source4_ldl.util.TableOperator;

public class CategoryItemIdProcessor extends AbstractTableProcessor {
    
    public CategoryItemIdProcessor(SqlExecutor sqlExecutor) {
        super(sqlExecutor);
    }
    
    @Override
    public void process() {
        // 1. 删除并创建 MergeTree 表
        sqlExecutor.executeSql(
            "DROP TABLE IF EXISTS tmp.itemid_alter_category_combine",
            "删除临时表"
        );
        
        sqlExecutor.executeSql(
            "CREATE TABLE tmp.itemid_alter_category_combine " +
            "ENGINE = MergeTree ORDER BY createtime AS " +
            "SELECT * FROM dim.itemid_alter_category_combine " +
            "ORDER BY createtime DESC LIMIT 1 BY Channel, Itemid",
            "创建MergeTree表"
        );
        
        // 2. 创建Join表
        tableOperator.createJoinTable(
            "tmp.itemid_alter_category_combine",
            "ods.itemid_alter_category_combine_dict",
            "Channel, Itemid"
        );
        
        // 3. 创建辅助表
        tableOperator.createAssistTable(
            "tmp.itemid_alter_category_combine",
            "ods.itemid_alter_category_combine_dict_assist"
        );
        
        // 4. 验证三个表数据
        dataValidator.validateThreeTableCounts(
            "tmp.itemid_alter_category_combine",
            "ods.itemid_alter_category_combine_dict",
            "ods.itemid_alter_category_combine_dict_assist"
        );
    }
    
    @Override
    public String getProcessorName() {
        return "category_itemid";
    }
    
    @Override
    public String getDescription() {
        return "分类ItemId处理";
    }
}

