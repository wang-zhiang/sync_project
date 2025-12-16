package hivesource.hive取数到ck

import hivesource.cksqlserverUtil
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession

object get_data_to_ck {
  def get_uaually_hive(hivetable: String): Unit = {

    System.setProperty("log4j.configuration", "D:\\wzza\\develop\\idea_project\\ceshi\\src\\main\\resources\\log4j2.xml")
    // 创建 Hadoop Configuration
    val hadoopConf = new Configuration()
    hadoopConf.addResource(new Path("D:\\wzza\\develop\\idea_project\\ceshi\\src\\main\\resources\\core-site.xml"))
    hadoopConf.addResource(new Path("D:\\wzza\\develop\\idea_project\\ceshi\\src\\main\\resources\\hdfs-site.xml"))
    hadoopConf.addResource(new Path("D:\\wzza\\develop\\idea_project\\ceshi\\src\\main\\resources\\yarn-site.xml"))
    hadoopConf.addResource(new Path("D:\\wzza\\develop\\idea_project\\ceshi\\src\\main\\resources\\hive-site.xml"))
    hadoopConf.addResource(new Path("D:\\wzza\\develop\\idea_project\\ceshi\\src\\main\\resources\\spark-defaults.xml"))

    // 应用 Hadoop 配置




    // 创建 SparkSession
    var spark = SparkSession.builder()
      .appName("Hive Access")
      //.config("spark.sql.warehouse.dir", "/user/hive/warehouse/dwd")
      .master("local[*]")
      .config("yarn.resourcemanager.scheduler.address", "hadoop107:8030")
      .enableHiveSupport()
      .getOrCreate()



    spark.sparkContext.hadoopConfiguration.addResource(hadoopConf)
    // import spark.implicits._
    //手动添加条件


    //val whereresult = "  where 1 = 1"
    val df = spark.sql(s""" select * from (
select
Province,
City,
shop_name as ShopName,
shop_id ShopId ,
CategoryFirst  CategoryFirst,
"" CategorySecond ,
coalesce(if(itemid ="",null,itemid),if(goods_id ="",null,goods_id)) ItemId ,
coalesce(if(ItemName ="",null,ItemName),if(goods_name ="",null,goods_name))ItemName ,
coalesce(if(promotion_price ="",null,promotion_price),0) Price ,
coalesce(if(origin_price ="",null,origin_price),0) PriceOriginal ,
coalesce(if(monthqty ="",null,monthqty),if(month_saled_content ="",null,regexp_extract(month_saled_content, "\\\\d+", 0)))  MonthQty ,
SaleValue ,
SaleValue000 ,
goods_inventory  Inventory ,
cast(update_time as timestamp) UpdateTime ,
id as OrgTableId ,
CASE
WHEN length(upccode) >2000 THEN substring(upccode,1,2000)
ELSE upccode end UPC ,
month_saled_content monthsales,
search_address ,
ttype ,
category_id categoryid ,
brandid,
"" sellerid,
"" shop ,
image_url  imgUrl ,
industryId,
adddate,
ser_num,
seriesid,
discount
from ${hivetable} where pt_ym = '202301' limit 10
  )
""")


    df.show()

    var schema = df.schema

    // Spark SQL 数据类型到 ClickHouse 数据类型的映射
    import org.apache.spark.sql.types._

    def sparkTypeToClickHouseType(sparkType: DataType): String = sparkType match {
      case IntegerType => "Int32"
      case LongType => "Int64"
      case DoubleType => "Float64"
      case BooleanType => "UInt8"
      case DateType => "Date"
      case TimestampType => "DateTime"
      // 添加更多数据类型的转换规则
      case _ => "String" // 默认转换为 String
    }

    // 构建 ClickHouse 建表语句
    var cktable = "mt_0626"
    val table_local =
      s"""$cktable"""
    var createTableStatement =
      s"""
  CREATE TABLE IF NOT EXISTS test.$cktable (
    ${schema.fields.map(f => s"${f.name} ${sparkTypeToClickHouseType(f.dataType)}").mkString(", ")}
  ) ENGINE = MergeTree()
  ORDER BY ${schema.fields.head.name}
"""
    cksqlserverUtil.ckEXECUTE(createTableStatement,"test",cktable)


    print("建表成功")
    // 输出建表语句
    println(createTableStatement)




    val clickhouseUrl1 = "jdbc:clickhouse://hadoop110:8123/test"
    val clickhouseUser = "default"
    val clickhousePassword = "smartpath"
    df
      .write
      .format("jdbc")
      .mode("append")
      .option("url", clickhouseUrl1)
      .option("fetchsize", "50")
      .option("driver", "ru.yandex.clickhouse.ClickHouseDriver")
      .option("user", clickhouseUser)
      .option("password", clickhousePassword)
      .option("dbtable", cktable)
      .save()
    spark.stop()

  }
}
