package kzb.source4_ldl.util;

public class DataValidator {
    private SqlExecutor sqlExecutor;
    
    public DataValidator(SqlExecutor sqlExecutor) {
        this.sqlExecutor = sqlExecutor;
    }
    
    // 通用的数据验证方法
    public void validateTableCounts(String table1, String table2) {
        long count1 = sqlExecutor.getTableCount(table1);
        long count2 = sqlExecutor.getTableCount(table2);
        
        System.out.println(String.format("%s 数据量: %d", table1, count1));
        System.out.println(String.format("%s 数据量: %d", table2, count2));
        
        if (count1 != count2) {
            throw new RuntimeException(String.format(
                "数据量不一致: %s(%d) vs %s(%d)", 
                table1, count1, table2, count2
            ));
        }
        System.out.println("✓ 数据验证通过");
    }
    
    public void validateThreeTableCounts(String table1, String table2, String table3) {
        long count1 = sqlExecutor.getTableCount(table1);
        long count2 = sqlExecutor.getTableCount(table2);
        long count3 = sqlExecutor.getTableCount(table3);
        
        System.out.println(String.format("%s 数据量: %d", table1, count1));
        System.out.println(String.format("%s 数据量: %d", table2, count2));
        System.out.println(String.format("%s 数据量: %d", table3, count3));
        
        if (count1 != count2 || count1 != count3) {
            throw new RuntimeException(String.format(
                "数据量不一致: %s(%d) vs %s(%d) vs %s(%d)", 
                table1, count1, table2, count2, table3, count3
            ));
        }
        System.out.println("✓ 三表数据验证通过");
    }
}