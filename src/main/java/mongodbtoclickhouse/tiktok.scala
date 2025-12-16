package mongodbtoclickhouse

import com.mongodb.spark.MongoSpark
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object tiktok {
  val clickhouseUser = "default"
  val clickhousePassword = "smartpath"

  def mongotock(clickhouseTable: String, clickhouseUrl: String, mongodb: String, mongodbbase: String): Unit = {
    val table = mongodb
    val mongobase = mongodbbase
    val dc = mongobase + "." + table
    println(dc)

    val sparkConf: SparkConf = new SparkConf()
      .setMaster("local[*]")
      .setAppName(this.getClass.getName)
      .set("spark.mongodb.input.uri", s"mongodb://smartpath:smartpthdata@192.168.5.101:27017,192.168.5.102:27017,192.168.5.103:27017/$dc")
      .set("spark.executor.memory", "4g")  // 每个Executor分配4GB内存
      .set("spark.driver.memory", "2g")    // Driver程序分配2GB内存
      .set("spark.executor.instances", "4")  // 启动4个Executor实例
      .set("spark.default.parallelism", "5")  // 设置并行度为10
      //.set("spark.sql.shuffle.partitions", "100")  // 设置Shuffle操作的分区数为100

    val sparkSession: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

    var df: DataFrame = MongoSpark.load(sparkSession)



    // 检查并替换包含“占比”的列名中的“占比”为“zb”
    df.columns.foreach { colName =>
      if (colName.contains("占比")) {
        val newColName = colName.replace("占比", "zb")
        df = df.withColumnRenamed(colName, newColName)
      }
    }

    // 将 DataFrame 中的所有列转换为字符串类型，并移除特殊字符，并将 _id 转换为 id
    val stringTypedDf = df.columns.foldLeft(
      df.withColumn("id", col("_id").cast("string"))
        .withColumn("id", regexp_extract(col("id"), "([0-9a-fA-F]{24})", 1))  // 提取ObjectId的字符串部分
        .drop("_id")  // 删除原始的 _id 列
    )((currentDf, colName) => {
      try {
        currentDf.withColumn(colName, regexp_replace(col(colName).cast("string"), "[\\n\\t\\r]", ""))
      } catch {
        case e: Exception =>
          println(s"Error converting column $colName to string: ${e.getMessage}")
          currentDf
      }
    })

    // 手动声明 MongoDB 中缺少的字段，并给它们设置默认值
    val finalDf = stringTypedDf
      .withColumn("server", lit("思勃达播自播-按月"))  // 示例：添加一个字段
      .withColumn("source_table", lit(mongodb))  // 示例：添加另一个字段
      .withColumn("pt_ym", lit("202408"))  // 示例：添加另一个字段
      .withColumn("pt_channel", lit("tiktok"))  // 示例：添加另一个字段

    finalDf.createOrReplaceTempView(table)

    val sqlquery = s"SELECT * FROM $table"
    val resDf = sparkSession.sql(sqlquery)
    resDf.show()

    // 写入数据到 ClickHouse
    resDf
      .write
      .format("jdbc")
      .mode("append")
      .option("url", clickhouseUrl)
      .option("fetchsize", "5000")
      .option("driver", "ru.yandex.clickhouse.ClickHouseDriver")
      .option("user", clickhouseUser)
      .option("password", clickhousePassword)
      .option("dbtable", clickhouseTable)
      .save()

    // 关闭 SparkSession
    sparkSession.stop()
  }
}
