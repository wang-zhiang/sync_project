package kzb;

import org.apache.poi.ss.usermodel.*;
import java.sql.SQLException;
import java.util.*;

public class CategoryJdSheetHandler extends SheetHandler {

    // 京东专用表名配置
    private static final String JD_DICT_TABLE = "ods.kzb_jd_category_dict";
    private static final String JD_ASSIST_TABLE = JD_DICT_TABLE + "_assist";
    private static final String JD_DIM_TABLE = "dim.jd_category_combine_tests";

    public CategoryJdSheetHandler(ClickHouseConfig ckConfig) {
        super(ckConfig);
    }

    @Override
    public void handleSheet(Sheet sheet) throws Exception {
        // 1. 解析数据（复用淘系逻辑）
        List<Map<String, String>> s4Data = parseS4Data(sheet);

        // 2. 处理S4类型数据（仅替换表名）
        if (!s4Data.isEmpty()) {
            dropTable(JD_DICT_TABLE);
            dropTable(JD_ASSIST_TABLE);
            createS4Tables(JD_DICT_TABLE, JD_ASSIST_TABLE);
            importS4Data(s4Data, JD_DICT_TABLE, JD_ASSIST_TABLE);
            updateMainTableForS4(JD_DICT_TABLE, JD_ASSIST_TABLE);
            insertDimensionTable(s4Data, JD_DIM_TABLE, "S4");
        }
    }

    //======================= 完全复用淘系S4处理逻辑 =======================//
    private List<Map<String, String>> parseS4Data(Sheet sheet) {
        List<Map<String, String>> data = new ArrayList<>();
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
            // 京东数据过滤逻辑与淘系一致
            if (rowData.get("LDL_categoryname2").equals("NULL") &&
                    !rowData.get("S4_rootcategoryname").equals("NULL")) {
                data.add(rowData);
            }
        }
        return data;
    }

    private void createS4Tables(String dictTable, String assistTable) throws SQLException {
        // 与淘系完全相同的建表语句
        String dictSql = String.format(
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

        String assistSql = String.format(
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
            ClickHouseUtil.executeUpdate(ckConfig.getConnectionUrl(node), dictSql);
            ClickHouseUtil.executeUpdate(ckConfig.getConnectionUrl(node), assistSql);
        }
    }

    private void importS4Data(List<Map<String, String>> data, String dictTable, String assistTable) throws SQLException {
        // 与淘系完全相同的导入逻辑
        for (Map<String, String> row : data) {
            String channel = row.get("Channel");
            String s4Root = row.get("S4_rootcategoryname");
            String s4Category = row.get("S4_categoryname");
            String s4Sub = row.get("S4_subcategoryname");
            String spI = row.get("SP_Category_I");
            String spII = row.get("SP_Category_II");
            String spIII = row.get("SP_Category_III");

            // 关联表
            String insertDict = String.format(
                    "INSERT INTO %s VALUES ('%s','%s','%s','%s','%s','%s','%s')",
                    dictTable, channel, s4Root, s4Category, s4Sub, spI, spII, spIII
            );

            // 辅助表（字段与淘系一致）
            String insertAssist = String.format(
                    "INSERT INTO %s VALUES ('%s','%s','%s','%s','%s','%s','%s')",
                    assistTable, channel, s4Root, s4Category, s4Sub, spI, spII, spIII
            );

            for (String node : ckConfig.getTargetNodes()) {
                ClickHouseUtil.executeUpdate(ckConfig.getConnectionUrl(node), insertDict);
                ClickHouseUtil.executeUpdate(ckConfig.getConnectionUrl(node), insertAssist);
            }
        }
    }

    private void updateMainTableForS4(String dictTable, String assistTable) throws SQLException {
        // 完全相同的更新逻辑
        String sql = String.format(
                "ALTER TABLE test.test_source4_ldl_local ON CLUSTER test_ck_cluster " +
                        "UPDATE SP_Category_I = joinGet('%s', 'SP_Category_I', Channel, S4_rootcategoryname, S4_categoryname, S4_subcategoryname), " +
                        "SP_Category_II = joinGet('%s', 'SP_Category_II', Channel, S4_rootcategoryname, S4_categoryname, S4_subcategoryname), " +
                        "SP_Category_III = joinGet('%s', 'SP_Category_III', Channel, S4_rootcategoryname, S4_categoryname, S4_subcategoryname) " +
                        "WHERE concat(Channel, S4_rootcategoryname, S4_categoryname, S4_subcategoryname) IN " +
                        "(SELECT concat(Channel, S4_rootcategoryname, S4_categoryname, S4_subcategoryname) FROM %s)",
                dictTable, dictTable, dictTable, assistTable
        );
        ClickHouseUtil.executeUpdate(ckConfig.getConnectionUrl("hadoop110"), sql);
    }

    private void insertDimensionTable(List<Map<String, String>> data, String dimTable, String type) throws SQLException {
        // 完全相同的维度表插入逻辑
        for (Map<String, String> row : data) {
            String sql = String.format(
                    "INSERT INTO %s (Channel, S4_rootcategoryname, S4_categoryname, S4_subcategoryname, " +
                            "SP_Category_I, SP_Category_II, SP_Category_III, createtime) " +
                            "VALUES ('%s','%s','%s','%s','%s','%s','%s',now())",
                    dimTable,
                    row.get("Channel"), row.get("S4_rootcategoryname"),
                    row.get("S4_categoryname"), row.get("S4_subcategoryname"),
                    row.get("SP_Category_I"), row.get("SP_Category_II"), row.get("SP_Category_III")
            );
            ClickHouseUtil.executeUpdate(ckConfig.getConnectionUrl("hadoop110"), sql);
        }
    }

    //======================= 复用工具方法 =======================//
    private void dropTable(String tableName) throws SQLException {
        String sql = "DROP TABLE IF EXISTS " + tableName;
        for (String node : ckConfig.getTargetNodes()) {
            ClickHouseUtil.executeUpdate(ckConfig.getConnectionUrl(node), sql);
        }
    }
}