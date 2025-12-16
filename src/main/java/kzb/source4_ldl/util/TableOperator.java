package kzb.source4_ldl.util;

public class TableOperator {
    private SqlExecutor sqlExecutor;
    
    public TableOperator(SqlExecutor sqlExecutor) {
        this.sqlExecutor = sqlExecutor;
    }
    
    // 通用的Join表创建方法
    public void createJoinTable(String sourceTable, String targetTable, String joinKeys) {
        // 删除已存在的Join表
        String dropSql = String.format("DROP TABLE IF EXISTS %s", targetTable);
        sqlExecutor.executeSql(dropSql, "删除Join表: " + targetTable);
        
        // 创建Join表
        String createSql = String.format(
            "CREATE TABLE %s ENGINE = Join(ANY, LEFT, %s) AS SELECT * FROM %s",
            targetTable, joinKeys, sourceTable
        );
        sqlExecutor.executeSql(createSql, "创建Join表: " + targetTable);
    }
    
    // 通用的辅助表创建方法
    public void createAssistTable(String sourceTable, String assistTable) {
        String dropSql = String.format("DROP TABLE IF EXISTS %s", assistTable);
        sqlExecutor.executeSql(dropSql, "删除辅助表: " + assistTable);
        
        String createSql = String.format(
            "CREATE TABLE %s ENGINE = MergeTree ORDER BY createtime AS SELECT * FROM %s",
            assistTable, sourceTable
        );
        sqlExecutor.executeSql(createSql, "创建辅助表: " + assistTable);
    }
}