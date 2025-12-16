package kzb;

import org.apache.poi.ss.usermodel.Sheet;


public abstract class SheetHandler {
    protected ClickHouseConfig ckConfig;

    public SheetHandler(ClickHouseConfig ckConfig) {
        this.ckConfig = ckConfig;
    }

    public abstract void handleSheet(Sheet sheet) throws Exception;
}