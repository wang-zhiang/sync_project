package kzb;

import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;

import java.sql.SQLException;
import java.util.*;

public class BrandSheetHandler extends SheetHandler {

    public BrandSheetHandler(ClickHouseConfig ckConfig) {
        super(ckConfig);
    }

    @Override
    public void handleSheet(Sheet sheet) throws Exception {
        // 1. 解析Sheet数据
        List<Map<String, String>> data = parseSheetData(sheet);

        // 2. 定义关联表和维度表
        String dictTable = "ods.kzb_brandname_dict";
        String dimensionTable = "dim.brandlist_all_new_tests";

        // 3. 删除旧表（如果存在）
        dropTable(dictTable);

        // 4. 创建关联表
        createDictTable(dictTable);

        // 5. 导入数据到关联表
        importDataToDictTable(data, dictTable);

        // 6. 更新大表
        updateMainTable(dictTable);

        // 7. 插入维度表
        insertDimensionTable(data, dimensionTable);
    }

    private List<Map<String, String>> parseSheetData(Sheet sheet) {
        List<Map<String, String>> data = new ArrayList<>();
        Iterator<Row> rowIterator = sheet.iterator();

        // 读取表头
        Row headerRow = rowIterator.next();
        List<String> headers = new ArrayList<>();
        for (Cell cell : headerRow) {
            headers.add(cell.getStringCellValue().replace("条件_", "").replace("结果_", ""));
        }

        // 读取数据行
        while (rowIterator.hasNext()) {
            Row row = rowIterator.next();
            Map<String, String> rowData = new HashMap<>();
            for (int i = 0; i < headers.size(); i++) {
                Cell cell = row.getCell(i, Row.MissingCellPolicy.CREATE_NULL_AS_BLANK);
                rowData.put(headers.get(i), cell.toString());
            }
            data.add(rowData);
        }

        return data;
    }

    private void dropTable(String tableName) throws SQLException {
        String dropTableSql = String.format("DROP TABLE IF EXISTS %s", tableName);

        for (String node : ckConfig.getTargetNodes()) {
            ClickHouseUtil.executeUpdate(ckConfig.getConnectionUrl(node), dropTableSql);
        }
    }

    private void createDictTable(String dictTable) throws SQLException {
        String createTableSql = String.format(
                "CREATE TABLE IF NOT EXISTS %s (Brandname String, SP_Brand_CNEN String, SP_Brand_CN String, SP_Brand_EN String) ENGINE = Join(ANY, LEFT, Brandname)",
                dictTable
        );

        for (String node : ckConfig.getTargetNodes()) {
            ClickHouseUtil.executeUpdate(ckConfig.getConnectionUrl(node), createTableSql);
        }
    }

    private void importDataToDictTable(List<Map<String, String>> data, String dictTable) throws SQLException {
        for (Map<String, String> row : data) {
            String brandname = row.get("Brandname");
            String spBrandCNEN = row.get("SP_Brand_CNEN");
            String spBrandCN = row.get("SP_Brand_CN");
            String spBrandEN = row.get("SP_Brand_EN");

            String insertSql = String.format(
                    "INSERT INTO %s (Brandname, SP_Brand_CNEN, SP_Brand_CN, SP_Brand_EN) VALUES ('%s', '%s', '%s', '%s')",
                    dictTable, brandname, spBrandCNEN, spBrandCN, spBrandEN
            );

            for (String node : ckConfig.getTargetNodes()) {
                ClickHouseUtil.executeUpdate(ckConfig.getConnectionUrl(node), insertSql);
            }
        }
    }

    private void updateMainTable(String dictTable) throws SQLException {
        // 更新大表的三条SQL语句
        String[] updateSqls = {
                String.format(
                        "ALTER TABLE test.test_source4_ldl_local ON CLUSTER test_ck_cluster UPDATE " +
                                "SP_Brand_CNEN = joinGet('%s', 'SP_Brand_CNEN', S4_brandname), " +
                                "SP_Brand_CN = joinGet('%s', 'SP_Brand_CN', S4_brandname), " +
                                "SP_Brand_EN = joinGet('%s', 'SP_Brand_EN', S4_brandname) " +
                                "WHERE pt_channel IN ('source4', 'ldl') AND   S4_brandname  IN (SELECT Brandname FROM %s)",
                        dictTable, dictTable, dictTable, dictTable
                ),
                String.format(
                        "ALTER TABLE test.test_source4_ldl_local ON CLUSTER test_ck_cluster UPDATE " +
                                "SP_Brand_CNEN = joinGet('%s', 'SP_Brand_CNEN', LDL_brand), " +
                                "SP_Brand_CN = joinGet('%s', 'SP_Brand_CN', LDL_brand), " +
                                "SP_Brand_EN = joinGet('%s', 'SP_Brand_EN', LDL_brand) " +
                                "WHERE pt_channel IN ('source4', 'ldl') AND LDL_brand  IN (SELECT Brandname FROM %s) " +
                                "AND S4_brandname  NOT IN (SELECT Brandname FROM %s)",
                        dictTable, dictTable, dictTable, dictTable, dictTable
                ),
                String.format(
                        "ALTER TABLE test.test_source4_ldl_local ON CLUSTER test_ck_cluster UPDATE " +
                                "SP_Brand_CNEN = joinGet('%s', 'SP_Brand_CNEN', DY_brandname), " +
                                "SP_Brand_CN = joinGet('%s', 'SP_Brand_CN', DY_brandname), " +
                                "SP_Brand_EN = joinGet('%s', 'SP_Brand_EN', DY_brandname) " +
                                "WHERE Channel = '抖音' AND DY_brandname  IN (SELECT Brandname FROM %s)",
                        dictTable, dictTable, dictTable, dictTable
                )
        };

        for (String sql : updateSqls) {
            System.out.println(sql);
            ClickHouseUtil.executeUpdate(ckConfig.getConnectionUrl("hadoop110"), sql);
        }
    }

    private void insertDimensionTable(List<Map<String, String>> data, String dimensionTable) throws SQLException {
        for (Map<String, String> row : data) {
            String brandname = row.get("Brandname");
            String spBrandCNEN = row.get("SP_Brand_CNEN");
            String spBrandCN = row.get("SP_Brand_CN");
            String spBrandEN = row.get("SP_Brand_EN");

            String insertSql = String.format(
                    "INSERT INTO %s (Brandname, SP_Brand_CNEN, SP_Brand_CN, SP_Brand_EN, updatetime) VALUES ('%s', '%s', '%s', '%s', now())",
                    dimensionTable, brandname, spBrandCNEN, spBrandCN, spBrandEN
            );

            ClickHouseUtil.executeUpdate(ckConfig.getConnectionUrl("hadoop110"), insertSql);
        }
    }
}