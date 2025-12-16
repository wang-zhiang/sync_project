package kzb;

import org.apache.poi.ss.usermodel.*;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ExcelReader implements AutoCloseable {
    private Workbook workbook;

    public ExcelReader(String filePath) throws IOException {
        this.workbook = WorkbookFactory.create(new File(filePath));
    }

    public Sheet getSheet(String sheetName) {
        return workbook.getSheet(sheetName);
    }

    @Override
    public void close() throws IOException {
        if (workbook != null) {
            workbook.close();
        }
    }
}