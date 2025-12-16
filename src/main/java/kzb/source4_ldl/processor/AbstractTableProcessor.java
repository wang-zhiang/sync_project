package kzb.source4_ldl.processor;

import kzb.source4_ldl.util.SqlExecutor;
import kzb.source4_ldl.util.TableOperator;
import kzb.source4_ldl.util.DataValidator;

public abstract class AbstractTableProcessor implements TableProcessor {
    protected SqlExecutor sqlExecutor;
    protected TableOperator tableOperator;
    protected DataValidator dataValidator;
    
    public AbstractTableProcessor(SqlExecutor sqlExecutor) {
        this.sqlExecutor = sqlExecutor;
        this.tableOperator = new TableOperator(sqlExecutor);
        this.dataValidator = new DataValidator(sqlExecutor);
    }
}