package kzb;

import org.apache.poi.ss.usermodel.*;
import java.sql.SQLException;
import java.util.*;

public class CategoryTaoxiSheetHandler extends SheetHandler {

    // 新增常量定义
    private static final String S4_ASSIST_TABLE = "ods.kzb_s4_category_dict_assist";

    public CategoryTaoxiSheetHandler(ClickHouseConfig ckConfig) {
        super(ckConfig);
    }

    @Override
    public void handleSheet(Sheet sheet) throws Exception {
        // 1. 解析数据并分类（保持原样）
        List<Map<String, String>> s4Data = new ArrayList<>();
        List<Map<String, String>> ldlData = new ArrayList<>();
        parseAndClassifyData(sheet, s4Data, ldlData);

        // 2. 处理S4类型数据（保持原样）
        if (!s4Data.isEmpty()) {
            String s4DictTable = "ods.kzb_s4_category_dict";
            String s4AssistTable = s4DictTable + "_assist";
            String s4DimensionTable = "dim.s4_category_combine_tests";

            dropTable(s4DictTable);
            dropTable(s4AssistTable);
            createS4Tables(s4DictTable, s4AssistTable);
            importS4Data(s4Data, s4DictTable, s4AssistTable);
            updateMainTableForS4(s4DictTable, s4AssistTable);
            insertDimensionTable(s4Data, s4DimensionTable, "S4");
        }

        // 3. 处理LDL类型数据（修复部分）
        if (!ldlData.isEmpty()) {
            String ldlDictTable = "ods.kzb_ldl_category_dict";
            String ldlAssistTable = ldlDictTable + "_assist"; // 新增辅助表
            String ldlDimensionTable = "dim.ldl_category_combine_tests";

            dropTable(ldlDictTable);
            dropTable(ldlAssistTable);
            createLDLTables(ldlDictTable, ldlAssistTable); // 修改表创建
            importLDLData(ldlData, ldlDictTable, ldlAssistTable); // 修改数据导入
            updateMainTableForLDL(ldlDictTable, ldlAssistTable); // 修改更新逻辑
            insertDimensionTable(ldlData, ldlDimensionTable, "LDL"); // 修改维度表插入
        }
    }

    //======================= 保持原有方法 =======================//
    private void parseAndClassifyData(Sheet sheet, List<Map<String, String>> s4Data, List<Map<String, String>> ldlData) {
        Iterator<Row> rowIterator = sheet.iterator();
        Row headerRow = rowIterator.next();
        List<String> headers = new ArrayList<>();
        for (Cell cell : headerRow) {
            headers.add(cell.getStringCellValue().replace("条件_", "").replace("结果_", ""));
        }

        while (rowIterator.hasNext()) {
            Row row = rowIterator.next();
            Map<String, String> rowData = new HashMap<>();
            for (int i = 0; i < headers.size(); i++) {
                Cell cell = row.getCell(i, Row.MissingCellPolicy.CREATE_NULL_AS_BLANK);
                rowData.put(headers.get(i), cell.toString());
            }

            boolean isS4 = rowData.get("LDL_categoryname2").equals("NULL") &&
                    !rowData.get("S4_rootcategoryname").equals("NULL");
            boolean isLDL = rowData.get("S4_rootcategoryname").equals("NULL") &&
                    !rowData.get("LDL_categoryname2").equals("NULL");

            if (isS4) {
                s4Data.add(rowData);
            } else if (isLDL) {
                ldlData.add(rowData);
            }
        }
    }

    private void createS4Tables(String dictTable, String assistTable) throws SQLException {
        String createDictSql = String.format(
                "CREATE TABLE IF NOT EXISTS %s (" +
                        "Channel String, " +
                        "S4_rootcategoryname String, " +
                        "S4_categoryname String, " +
                        "S4_subcategoryname String, " +
                        "SP_Category_I String, " +
                        "SP_Category_II String, " +
                        "SP_Category_III String" +
                        ") ENGINE = Join(ANY, LEFT, Channel, S4_rootcategoryname, S4_categoryname, S4_subcategoryname)",
                dictTable
        );

        String createAssistSql = String.format(
                "CREATE TABLE IF NOT EXISTS %s (" +
                        "Channel String, " +
                        "S4_rootcategoryname String, " +
                        "S4_categoryname String, " +
                        "S4_subcategoryname String," +
                        "SP_Category_I String, " +
                        "SP_Category_II String, " +
                        "SP_Category_III String" +
                        ") ENGINE = MergeTree ORDER BY Channel",
                assistTable
        );

        for (String node : ckConfig.getTargetNodes()) {
            ClickHouseUtil.executeUpdate(ckConfig.getConnectionUrl(node), createDictSql);
            ClickHouseUtil.executeUpdate(ckConfig.getConnectionUrl(node), createAssistSql);
        }
    }

    private void importS4Data(List<Map<String, String>> data, String dictTable, String assistTable) throws SQLException {
        for (Map<String, String> row : data) {
            String channel = row.get("Channel");
            String s4Root = row.get("S4_rootcategoryname");
            String s4Category = row.get("S4_categoryname");
            String s4Sub = row.get("S4_subcategoryname");
            String spI = row.get("SP_Category_I");
            String spII = row.get("SP_Category_II");
            String spIII = row.get("SP_Category_III");

            String insertDictSql = String.format(
                    "INSERT INTO %s VALUES ('%s', '%s', '%s', '%s', '%s', '%s', '%s')",
                    dictTable, channel, s4Root, s4Category, s4Sub, spI, spII, spIII
            );

            String insertAssistSql = String.format(
                    "INSERT INTO %s VALUES ('%s', '%s', '%s', '%s', '%s', '%s', '%s')",
                    assistTable, channel, s4Root, s4Category, s4Sub, s4Sub, spI, spII, spIII
            );

            for (String node : ckConfig.getTargetNodes()) {
                ClickHouseUtil.executeUpdate(ckConfig.getConnectionUrl(node), insertDictSql);
                ClickHouseUtil.executeUpdate(ckConfig.getConnectionUrl(node), insertAssistSql);
            }
        }
    }

    private void updateMainTableForS4(String dictTable, String assistTable) throws SQLException {
        String updateSql = String.format(
                "ALTER TABLE test.test_source4_ldl_local ON CLUSTER test_ck_cluster " +
                        "UPDATE SP_Category_I = joinGet('%s', 'SP_Category_I', Channel, S4_rootcategoryname, S4_categoryname, S4_subcategoryname), " +
                        "SP_Category_II = joinGet('%s', 'SP_Category_II', Channel, S4_rootcategoryname, S4_categoryname, S4_subcategoryname), " +
                        "SP_Category_III = joinGet('%s', 'SP_Category_III', Channel, S4_rootcategoryname, S4_categoryname, S4_subcategoryname) " +
                        "WHERE concat(Channel, S4_rootcategoryname, S4_categoryname, S4_subcategoryname) IN " +
                        "(SELECT concat(Channel, S4_rootcategoryname, S4_categoryname, S4_subcategoryname) FROM %s)",
                dictTable, dictTable, dictTable, assistTable
        );
        System.out.println(updateSql);
        ClickHouseUtil.executeUpdate(ckConfig.getConnectionUrl("hadoop110"), updateSql);
    }

    //======================= 修复的LDL部分 =======================//
    private void createLDLTables(String dictTable, String assistTable) throws SQLException {
        // 修复1：表结构包含Channel字段
        String dictSql = String.format(
                "CREATE TABLE IF NOT EXISTS %s (" +
                        "Channel String, " +  // 新增Channel字段
                        "LDL_categoryname2 String, " +
                        "SP_Category_I String, " +
                        "SP_Category_II String, " +
                        "SP_Category_III String" +
                        ") ENGINE = Join(ANY, LEFT, Channel, LDL_categoryname2)",  // 修改JOIN键
                dictTable
        );

        String assistSql = String.format(
                "CREATE TABLE IF NOT EXISTS %s (" +
                        "Channel String, " +  // 新增Channel字段
                        "LDL_categoryname2 String" +
                        ") ENGINE = MergeTree ORDER BY Channel",
                assistTable
        );

        for (String node : ckConfig.getTargetNodes()) {
            ClickHouseUtil.executeUpdate(ckConfig.getConnectionUrl(node), dictSql);
            ClickHouseUtil.executeUpdate(ckConfig.getConnectionUrl(node), assistSql);
        }
    }

    private void importLDLData(List<Map<String, String>> data, String dictTable, String assistTable) throws SQLException {
        // 修复2：插入数据包含Channel字段
        for (Map<String, String> row : data) {
            String channel = row.get("Channel");
            String ldlCategory = row.get("LDL_categoryname2");
            String spI = row.get("SP_Category_I");
            String spII = row.get("SP_Category_II");
            String spIII = row.get("SP_Category_III");

            // 关联表插入
            String dictInsert = String.format(
                    "INSERT INTO %s VALUES ('%s','%s','%s','%s','%s')",  // 增加Channel
                    dictTable, channel, ldlCategory, spI, spII, spIII
            );

            // 辅助表插入
            String assistInsert = String.format(
                    "INSERT INTO %s VALUES ('%s','%s')",  // 增加Channel
                    assistTable, channel, ldlCategory
            );

            for (String node : ckConfig.getTargetNodes()) {
                ClickHouseUtil.executeUpdate(ckConfig.getConnectionUrl(node), dictInsert);
                ClickHouseUtil.executeUpdate(ckConfig.getConnectionUrl(node), assistInsert);
            }
        }
    }

    private void updateMainTableForLDL(String dictTable, String ldlAssistTable) throws SQLException {
        // 修复3：修正参数顺序
        String[] updateSqls = {
                // 第一个UPDATE
                String.format(
                        "ALTER TABLE test.test_source4_ldl_local ON CLUSTER test_ck_cluster " +
                                "UPDATE SP_Category_I = joinGet('%s', 'SP_Category_I', Channel, LDL_categoryname2), " +
                                "SP_Category_II = joinGet('%s', 'SP_Category_II', Channel, LDL_categoryname2), " +
                                "SP_Category_III = joinGet('%s', 'SP_Category_III', Channel, LDL_categoryname2) " +
                                "WHERE Channel IN ('淘宝','天猫') AND LDL_categoryname2 IN (SELECT LDL_categoryname2 FROM %s) " + // LDL辅助表
                                "AND concat(S4_rootcategoryname, S4_categoryname, S4_subcategoryname) NOT IN " +
                                "(SELECT concat(S4_rootcategoryname, S4_categoryname, S4_subcategoryname) FROM %s)", // S4辅助表
                        dictTable, dictTable, dictTable,
                        ldlAssistTable,    // 第四个参数
                        S4_ASSIST_TABLE     // 第五个参数
                ),
                // 第二个UPDATE
                String.format(
                        "ALTER TABLE test.test_source4_ldl_local ON CLUSTER test_ck_cluster " +
                                "UPDATE SP_Category_I = joinGet('%s', 'SP_Category_I', Channel, LDL_categoryname1), " +
                                "SP_Category_II = joinGet('%s', 'SP_Category_II', Channel, LDL_categoryname1), " +
                                "SP_Category_III = joinGet('%s', 'SP_Category_III', Channel, LDL_categoryname1) " +
                                "WHERE Channel IN ('淘宝','天猫') AND LDL_categoryname1 IN (SELECT LDL_categoryname2 FROM %s) " + // LDL关联表
                                "AND LDL_categoryname2 NOT IN (SELECT LDL_categoryname2 FROM %s) " + // LDL辅助表
                                "AND concat(S4_rootcategoryname, S4_categoryname, S4_subcategoryname) NOT IN " +
                                "(SELECT concat(S4_rootcategoryname, S4_categoryname, S4_subcategoryname) FROM %s)", // S4辅助表
                        dictTable, dictTable, dictTable,
                        ldlAssistTable,     // 第四个参数（关联表）
                        ldlAssistTable,// 第五个参数（辅助表）
                        S4_ASSIST_TABLE// 第六个参数
                )
        };

        for (String sql : updateSqls) {
            System.out.println(sql);
            ClickHouseUtil.executeUpdate(ckConfig.getConnectionUrl("hadoop110"), sql);
        }
    }

    private void insertDimensionTable(List<Map<String, String>> data, String dimensionTable, String type) throws SQLException {
        // 修复4：维度表插入包含Channel字段
        for (Map<String, String> row : data) {
            String insertSql;
            if ("S4".equals(type)) {
                insertSql = String.format(
                        "INSERT INTO %s (Channel, S4_rootcategoryname, S4_categoryname, S4_subcategoryname, SP_Category_I, SP_Category_II, SP_Category_III, createtime) " +
                                "VALUES ('%s', '%s', '%s', '%s', '%s', '%s', '%s', now())",
                        dimensionTable,
                        row.get("Channel"), row.get("S4_rootcategoryname"), row.get("S4_categoryname"),
                        row.get("S4_subcategoryname"), row.get("SP_Category_I"), row.get("SP_Category_II"), row.get("SP_Category_III")
                );
            } else {
                insertSql = String.format(
                        "INSERT INTO %s (Channel, LDL_categoryname2, SP_Category_I, SP_Category_II, SP_Category_III, createtime) " + // 增加Channel
                                "VALUES ('%s','%s', '%s', '%s', '%s', now())",
                        dimensionTable,
                        row.get("Channel"),  // 新增
                        row.get("LDL_categoryname2"),
                        row.get("SP_Category_I"),
                        row.get("SP_Category_II"),
                        row.get("SP_Category_III")
                );
            }
            ClickHouseUtil.executeUpdate(ckConfig.getConnectionUrl("hadoop110"), insertSql);
        }
    }

    //======================= 保持原有工具方法 =======================//
    private void dropTable(String tableName) throws SQLException {
        String dropTableSql = String.format("DROP TABLE IF EXISTS %s", tableName);
        for (String node : ckConfig.getTargetNodes()) {
            ClickHouseUtil.executeUpdate(ckConfig.getConnectionUrl(node), dropTableSql);
        }
    }
}