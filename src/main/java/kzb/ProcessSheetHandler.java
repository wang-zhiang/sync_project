package kzb;

import org.apache.poi.ss.usermodel.*;

import java.sql.SQLException;
import java.util.*;

public class ProcessSheetHandler extends SheetHandler {

    public ProcessSheetHandler(ClickHouseConfig ckConfig) {
        super(ckConfig);
    }

    @Override
    public void handleSheet(Sheet sheet) throws Exception {
        // 1. 解析Sheet数据
        List<Map<String, String>> data = parseSheetData(sheet);

        // 2. 检查YMBEGIN和YMEND是否一致
        validateYmValues(data);

        // 3. 判断是修改Brand还是Category
        boolean isBrandUpdate = isBrandUpdate(data);

        // 4. 动态生成表名
        String dictTable = isBrandUpdate ? "ods.kzb_brand_itemid_dict" : "ods.kzb_category_itemid_dict";
        String assistTable = isBrandUpdate ? "ods.kzb_brand_itemid_dict_assist" : "ods.kzb_category_itemid_dict_assist";

        // 5. 删除旧表（如果存在）
        dropTables(dictTable, assistTable);

        // 6. 创建CK关联表和辅助表
        createCkTables(dictTable, assistTable, isBrandUpdate);

        // 7. 导入数据到CK关联表和辅助表
        importDataToCk(data, dictTable, assistTable, isBrandUpdate);

        // 8. 更新大表
        updateMainTable(dictTable, assistTable, isBrandUpdate,data);

//        // 9. 插入维度表
        insertDimensionTable(data, isBrandUpdate);
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

    private void validateYmValues(List<Map<String, String>> data) {
        String ymBegin = data.get(0).get("YMBEGIN");
        String ymEnd = data.get(0).get("YMEND");

        for (Map<String, String> row : data) {
            if (!row.get("YMBEGIN").equals(ymBegin) || !row.get("YMEND").equals(ymEnd)) {
                throw new RuntimeException("YMBEGIN或YMEND不一致");
            }
        }
    }

    private boolean isBrandUpdate(List<Map<String, String>> data) {
        return data.get(0).containsKey("SP_Brand_CNEN");
    }

    private void dropTables(String dictTable, String assistTable) throws SQLException {
        String dropDictTableSql = String.format("DROP TABLE IF EXISTS %s", dictTable);
        String dropAssistTableSql = String.format("DROP TABLE IF EXISTS %s", assistTable);

        for (String node : ckConfig.getTargetNodes()) {
            ClickHouseUtil.executeUpdate(ckConfig.getConnectionUrl(node), dropDictTableSql);
            ClickHouseUtil.executeUpdate(ckConfig.getConnectionUrl(node), dropAssistTableSql);
        }
    }

    private void createCkTables(String dictTable, String assistTable, boolean isBrandUpdate) throws SQLException {
        String dictTableSql = String.format(
                "CREATE TABLE IF NOT EXISTS %s (Channel String, Itemid String, %s) ENGINE = Join(ANY, LEFT, Channel, Itemid)",
                dictTable,
                isBrandUpdate ? "SP_Brand_CNEN String, SP_Brand_CN String, SP_Brand_EN String" :
                        "SP_Category_I String, SP_Category_II String, SP_Category_III String"
        );

        String assistTableSql = String.format(
                "CREATE TABLE IF NOT EXISTS %s (Channel String, Itemid String, %s) ENGINE = MergeTree ORDER BY Channel SETTINGS index_granularity = 8192",
                assistTable,
                isBrandUpdate ? "SP_Brand_CNEN String, SP_Brand_CN String, SP_Brand_EN String" :
                        "SP_Category_I String, SP_Category_II String, SP_Category_III String"
        );

        for (String node : ckConfig.getTargetNodes()) {
            ClickHouseUtil.executeUpdate(ckConfig.getConnectionUrl(node), dictTableSql);
            ClickHouseUtil.executeUpdate(ckConfig.getConnectionUrl(node), assistTableSql);
        }
    }

    private void importDataToCk(List<Map<String, String>> data, String dictTable, String assistTable, boolean isBrandUpdate) throws SQLException {
        for (Map<String, String> row : data) {
            String channel = row.get("Channel");
            String itemid = row.get("Itemid");

            String insertDictTableSql = String.format(
                    "INSERT INTO %s (Channel, Itemid, %s) VALUES ('%s', '%s', %s)",
                    dictTable,
                    isBrandUpdate ? "SP_Brand_CNEN, SP_Brand_CN, SP_Brand_EN" :
                            "SP_Category_I, SP_Category_II, SP_Category_III",
                    channel, itemid,
                    isBrandUpdate ? String.format("'%s', '%s', '%s'", row.get("SP_Brand_CNEN"), row.get("SP_Brand_CN"), row.get("SP_Brand_EN")) :
                            String.format("'%s', '%s', '%s'", row.get("SP_Category_I"), row.get("SP_Category_II"), row.get("SP_Category_III"))
            );

            String insertAssistTableSql = String.format(
                    "INSERT INTO %s (Channel, Itemid, %s) VALUES ('%s', '%s', %s)",
                    assistTable, isBrandUpdate ? "SP_Brand_CNEN, SP_Brand_CN, SP_Brand_EN" :
                            "SP_Category_I, SP_Category_II, SP_Category_III",
                    channel, itemid,
                    isBrandUpdate ? String.format("'%s', '%s', '%s'", row.get("SP_Brand_CNEN"), row.get("SP_Brand_CN"), row.get("SP_Brand_EN")) :
                            String.format("'%s', '%s', '%s'", row.get("SP_Category_I"), row.get("SP_Category_II"), row.get("SP_Category_III"))
            );

            for (String node : ckConfig.getTargetNodes()) {
                ClickHouseUtil.executeUpdate(ckConfig.getConnectionUrl(node), insertDictTableSql);
                ClickHouseUtil.executeUpdate(ckConfig.getConnectionUrl(node), insertAssistTableSql);
            }
        }
    }

    private void updateMainTable(String dictTable, String assistTable, boolean isBrandUpdate, List<Map<String, String>> data) throws SQLException {
        // 从数据中获取 YMBEGIN 和 YMEND 的值
        String ymBegin = data.get(0).get("YMBEGIN");
        String ymEnd = data.get(0).get("YMEND");
        System.out.println("dictTable: " + dictTable);
        System.out.println("assistTable: " + assistTable);

        // 构建更新 SQL 语句
        String updateSql;
        if (isBrandUpdate) {
            updateSql = String.format(
                    "ALTER TABLE test.test_source4_ldl_local ON CLUSTER test_ck_cluster UPDATE " +
                            "SP_Brand_CNEN = joinGet('%s', 'SP_Brand_CNEN', Channel, Itemid), " +
                            "SP_Brand_CN = joinGet('%s', 'SP_Brand_CN', Channel, Itemid), " +
                            "SP_Brand_EN = joinGet('%s', 'SP_Brand_EN', Channel, Itemid) " +
                            "WHERE pt_ym >= '%s' AND pt_ym <= '%s' " +
                            "AND concat(Channel, Itemid) IN (SELECT concat(Channel, Itemid) FROM %s)",
                    dictTable, dictTable, dictTable, ymBegin, ymEnd, assistTable
            );
        } else {
            updateSql = String.format(
                    "ALTER TABLE test.test_source4_ldl_local ON CLUSTER test_ck_cluster UPDATE " +
                            "SP_Category_I = joinGet('%s', 'SP_Category_I', Channel, Itemid), " +
                            "SP_Category_II = joinGet('%s', 'SP_Category_II', Channel, Itemid), " +
                            "SP_Category_III = joinGet('%s', 'SP_Category_III', Channel, Itemid) " +
                            "WHERE pt_ym >= '%s' AND pt_ym <= '%s' " +
                            "AND concat(Channel, Itemid) IN (SELECT concat(Channel, Itemid) FROM %s)",
                    dictTable, dictTable, dictTable, ymBegin, ymEnd, assistTable
            );
        }

        System.out.println("更新SQL语句输出: " + updateSql);

        // 执行更新操作
        ClickHouseUtil.executeUpdate(ckConfig.getConnectionUrl("hadoop110"), updateSql);
    }

    private void insertDimensionTable(List<Map<String, String>> data, boolean isBrandUpdate) throws SQLException {
        String dimensionTable = isBrandUpdate ? "dim.itemid_brand_combine_tests" : "dim.itemid_alter_category_combine_tests";
        for (Map<String, String> row : data) {
            String insertSql = String.format(
                    "INSERT INTO %s (Channel, Itemid, %s, createtime) VALUES ('%s', '%s', %s, now())",
                    dimensionTable,
                    isBrandUpdate ? "SP_Brand_CNEN, SP_Brand_CN, SP_Brand_EN" :
                            "SP_Category_I, SP_Category_II, SP_Category_III",
                    row.get("Channel"), row.get("Itemid"),
                    isBrandUpdate ? String.format("'%s', '%s', '%s'", row.get("SP_Brand_CNEN"), row.get("SP_Brand_CN"), row.get("SP_Brand_EN")) :
                            String.format("'%s', '%s', '%s'", row.get("SP_Category_I"), row.get("SP_Category_II"), row.get("SP_Category_III"))
            );

            ClickHouseUtil.executeUpdate(ckConfig.getConnectionUrl("hadoop110"), insertSql);
        }
    }
}