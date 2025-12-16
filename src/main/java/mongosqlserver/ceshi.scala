package mongosqlserver


import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import com.mongodb.spark.MongoSpark


object ceshi {
  def main(args: Array[String]): Unit = {
    val table  = "tm_sku_price_20231101_231010month"
    val dc = "ec.tm_sku_price_20231101_231010month"
    //System.setProperty("hadoop.home.dir", "D:\\wzza\\develop\\hadoop\\hadoop-3.1.3")
    val sparkConf: SparkConf = new SparkConf()
    val uri: String = "mongodb://smartpath:smartpthdata@192.168.5.101:27017,192.168.5.102:27017,192.168.5.103:27017/" + dc

    sparkConf.setMaster("local[*]").setAppName(this.getClass.getName)
    //sparkConf.setAppName(this.getClass.getName)
    sparkConf.set("spark.mongodb.input.uri", uri)


    val sparkSession: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    // sparkSession.sparkContext.setLogLevel("warn")
    val df: DataFrame = MongoSpark.load(sparkSession)
    //println(df.show())
    import org.apache.spark.sql.functions._

    // 将 DataFrame 中的所有列转换为字符串类型
    val stringTypedDf = df.columns.foldLeft(df)((currentDf, colName) =>
      currentDf.withColumn(colName, col(colName).cast("string"))
    )
    stringTypedDf.createOrReplaceTempView("user")
    //    df.createOrReplaceTempView("user")
    //   originaltable  replace(substr(tdate,1,10),'-','')  ymd   eplace(substr(tdate,1,7),'-','')  ym
    val resDf = sparkSession.sql( s""" SELECT '$table' as originaltable, replace(substr(tdate, 1, 10), '-', '') as ymd, replace(substr(tdate, 1, 7), '-', '') as ym,regexp_replace(itemname, '[\\n\\t\\r]', '') as itemname, regexp_replace(SellerId, '[\\n\\t\\r]', '') as SellerId, regexp_replace(selleruserid, '[\\n\\t\\r]', '') as selleruserid, regexp_replace(price, '[\\n\\t\\r]', '') as price, regexp_replace(qty, '[\\n\\t\\r]', '') as qty, regexp_replace(preqty, '[\\n\\t\\r]', '') as preqty, regexp_replace(successqty, '[\\n\\t\\r]', '') as successqty, regexp_replace(itemid, '[\\n\\t\\r]', '') as itemid, regexp_replace(grand, '[\\n\\t\\r]', '') as grand, regexp_replace(tdate, '[\\n\\t\\r]', '') as tdate, regexp_replace(skuname, '[\\n\\t\\r]', '') as skuname, regexp_replace(skuid, '[\\n\\t\\r]', '') as skuid, regexp_replace(batchId, '[\\n\\t\\r]', '') as batchId, regexp_replace(imgurl, '[\\n\\t\\r]', '') as imgurl, regexp_replace(skuimgurl, '[\\n\\t\\r]', '') as skuimgurl, regexp_replace(itemInfo, '[\\n\\t\\r]', '') as itemInfo, regexp_replace(skuquantity, '[\\n\\t\\r]', '') as skuquantity, regexp_replace(shopname, '[\\n\\t\\r]', '') as shopname, regexp_replace(userid, '[\\n\\t\\r]', '') as userid, regexp_replace(shoptime, '[\\n\\t\\r]', '') as shoptime, regexp_replace(shopscore, '[\\n\\t\\r]', '') as shopscore, regexp_replace(skucode, '[\\n\\t\\r]', '') as skucode, regexp_replace(quanhouPrice, '[\\n\\t\\r]', '') as quanhouPrice, regexp_replace(extensionInfo, '[\\n\\t\\r]', '') as extensionInfo FROM user""")

    // 获取 DataFrame 的 schema
    val schema = resDf.schema

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
      case _ => "String"  // 默认转换为 String
    }

    // 构建 ClickHouse 建表语句
    val createTableStatement = s"""
  CREATE TABLE IF NOT EXISTS my_clickhouse_table (
    ${schema.fields.map(f => s"${f.name} ${sparkTypeToClickHouseType(f.dataType)}").mkString(", ")}
  ) ENGINE = MergeTree()
  ORDER BY ${schema.fields.head.name}
"""

    // 输出建表语句
    println(createTableStatement)
    //    df.printSchema()
    //    resDf.show()
    //    val clickhouseUrl = "jdbc:clickhouse://hadoop110:8123/ods"
    //    val clickhouseTable = "tm_app_shop"
    //    val clickhouseUser = "default"
    //    val clickhousePassword = "smartpath"
    //
    //
    ////    //    //这里应该有个trycathch 失败的原因可以写在里面
    //    resDf
    //      .write
    //      .format("jdbc")
    //      .mode("append")
    //      .option("url", clickhouseUrl)
    //      .option("fetchsize", "500000")
    //      .option("driver", "ru.yandex.clickhouse.ClickHouseDriver")
    //      .option("user", clickhouseUser)
    //      .option("password", clickhousePassword)
    //      .option("dbtable", clickhouseTable)
    //      .save()

  }

}
