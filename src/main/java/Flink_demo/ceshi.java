package Flink_demo;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class ceshi {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        env.setParallelism(1);

        // 完整字段定义（严格对应SQL Server表结构）
        String sourceDDL = "CREATE TABLE skuall_ceshi_cdc (\n" +
                // --- 基础字段 ---
                "    Id BIGINT,\n" +
                "    Seriesid BIGINT,\n" +
                "    IndustryId INT,\n" +
                "    CategoryId INT,\n" +
                "    BrandId INT,\n" +
                "    SKU STRING,\n" +
                "    Package STRING,\n" +
                "    Imported INT,\n" +
                "    Format STRING,\n" +
                "    SizeId INT,\n" +
                "    QtyId INT,\n" +
                "    Unit STRING,\n" +
                "    Tier FLOAT,\n" +
                "    SpecId INT,\n" +
                "    Singleskuid BIGINT,\n" +

                // --- 功能字段 ---
                "    FUNCTION1 STRING,\n" +
                "    FUNCTION2 STRING,\n" +
                "    FUNCTION3 STRING,\n" +
                "    FUNCTION4 STRING,\n" +
                "    Abbdef STRING,\n" +
                "    WyethTier STRING,\n" +
                "    MJNpricetier STRING,\n" +
                "    Official STRING,\n" +

                // --- 关键字段 ---
                "    Key1 STRING,\n" +
                "    Key2 STRING,\n" +
                "    Key3 STRING,\n" +
                "    Key4 STRING,\n" +
                "    Key5 STRING,\n" +
                "    Key6 STRING,\n" +

                // --- 业务属性 ---
                "    SpecialtyStage INT,\n" +
                "    Countries STRING,\n" +
                "    Country STRING,\n" +
                "    VariantTier INT,\n" +
                "    DanoneTier INT,\n" +
                "    MaxPrice DECIMAL(18,2),\n" +
                "    MinPrice DECIMAL(18,2),\n" +
                "    MJNimport STRING,\n" +
                "    WyethNestlestage STRING,\n" +
                "    Abbottimported INT,\n" +
                "    key7 STRING,\n" +
                "    key8 STRING,\n" +

                // --- 商品管理 ---
                "    QtyIdNew INT,\n" +
                "    skugroupid BIGINT,\n" +
                "    MilkSource STRING,\n" +
                "    Production STRING,\n" +
                "    CountryofSale STRING,\n" +
                "    ABStage STRING,\n" +
                "    ABImported INT,\n" +
                "    type INT,\n" +
                "    skuidnew BIGINT,\n" +

                // --- 品牌相关 ---
                "    A2 STRING,\n" +
                "    Valio STRING,\n" +
                "    Manufacturer1 STRING,\n" +
                "    brandEN STRING,\n" +
                "    `A2 分类` STRING,\n" +
                "    `A2 Segment` STRING,\n" +

                // --- 规格属性 ---
                "    packsize FLOAT,\n" +
                "    skuadddate TIMESTAMP(3),\n" +

                // --- 品类特征 ---
                "    ShiseidoCategory STRING,\n" +
                "    ShiseidoFunction1 STRING,\n" +
                "    ShiseidoFunction2 STRING,\n" +
                "    `SingleSKU Name` STRING,\n" +
                "    `Bayer CountryofSale` BIGINT,\n" +

                // --- 功能扩展 ---
                "    Function5 STRING,\n" +
                "    Function6 STRING,\n" +
                "    Function7 STRING,\n" +
                "    `Price Cup` DECIMAL(18,2),\n" +
                "    `Price ML` DECIMAL(18,2),\n" +

                // --- 区域特征 ---
                "    `Country-YP` STRING,\n" +
                "    `BY FORMAT` STRING,\n" +

                // --- 功能细分 ---
                "    `1.Oil control 控油` STRING,\n" +
                "    `2.Basic hydration 基础保湿` STRING,\n" +
                "    `3.Anti-dandruff 去屑` STRING,\n" +
                "    `4.Anti-hair fall 头皮防脱` STRING,\n" +
                "    `5.Fiber Repair 头发修复` STRING,\n" +
                "    `6.Color lock 锁色` STRING,\n" +
                "    `7.Scalp care 养护头皮` STRING,\n" +
                "    `8.Inner Repair强韧发芯` STRING,\n" +
                "    `7.1Soothing 镇定头皮` STRING,\n" +
                "    `7.2Anti-aging 头皮抗老` STRING,\n" +
                "    `7.3Peeling 头皮去角质` STRING,\n" +
                "    `7.4Scalp repair 头皮修护` STRING,\n" +
                "    `7.5General scalp care 普通头皮养护` STRING,\n" +
                "    `8.1Strengthen 强韧` STRING,\n" +
                "    `8.2Anti-break 防断发` STRING,\n" +
                "    `8.3Structure reduction结构还原` STRING,\n" +

                // --- 特殊属性 ---
                "    `防脱特证` STRING,\n" +
                "    `Fluffing & Volumnizing 蓬松` STRING,\n" +
                "    Crudflag STRING,\n" +

                // --- CDC元数据 ---
                "    op_type STRING METADATA FROM 'op_type',\n" +
                "    op_ts TIMESTAMP_LTZ(3) METADATA FROM 'op_ts',\n" +
                "    PRIMARY KEY (Id) NOT ENFORCED\n" +
                ") WITH (\n" +
                "    'connector' = 'sqlserver-cdc',\n" +
                "    'hostname' = '192.168.4.218',\n" +
                "    'port' = '2599',\n" +
                "    'username' = 'CHH',\n" +
                "    'password' = 'Y1v606',\n" +
                "    'database-name' = 'datasystem',\n" +
                "    'schema-name' = 'dbo',\n" +
                "    'table-name' = 'skuall_ceshi',\n" +
                "    'scan.startup.mode' = 'initial',\n" +
                "    'debezium.snapshot.mode' = 'initial',\n" +
                "    'debezium.logging.context.name' = 'full_cdc_monitor'\n" +
                ")";

        tableEnv.executeSql(sourceDDL);

        // 全字段变更检测查询
        String fullFieldQuery =
                "SELECT \n" +
                        "  op_type, \n" +
                        "  op_ts, \n" +
                        "  TO_JSON(COALESCE(after, before)) AS full_record, \n" +  // 完整记录
                        "  CASE op_type\n" +
                        "    WHEN 'INSERT' THEN '新增记录: ' || CAST(Id AS STRING)\n" +
                        "    WHEN 'UPDATE' THEN '修改记录: ' || CAST(Id AS STRING) || ' 差异字段: ' || \n" +
                        "      ARRAY_JOIN(\n" +
                        "        MAP_KEYS(MAP_DIFF(TO_MAP(before), TO_MAP(after))), ', ')\n" +
                        "    WHEN 'DELETE' THEN '删除记录: ' || CAST(Id AS STRING)\n" +
                        "  END AS operation_detail\n" +
                        "FROM skuall_ceshi_cdc";

        tableEnv.executeSql(fullFieldQuery).print();

        env.execute("Complete SQL Server CDC Monitoring");
    }
}