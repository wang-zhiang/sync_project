package mongodbtoclickhouse

import java.sql.DriverManager

import cktosqlserverutil.cktosql.{clickhousePassword, clickhouseUser}
import com.mongodb.spark.MongoSpark
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object mongotock {
  val clickhouseUser = "default"
  val clickhousePassword = "smartpath"

  def mongotock(clickhouseTable: String, clickhouseUrl: String, mongodb: String, mongodbbase: String): Unit = {
    var table = mongodb
    var mongobase = mongodbbase
    var dc = mongobase + "." + table
    println(dc)

    var sparkConf: SparkConf = new SparkConf()
    var uri: String = "mongodb://smartpath:smartpthdata@192.168.5.101:27017,192.168.5.102:27017,192.168.5.103:27017/" + dc

    sparkConf.setMaster("local[*]").setAppName(this.getClass.getName)
    sparkConf.set("spark.mongodb.input.uri", uri)

    var sparkSession: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

    var df: DataFrame = MongoSpark.load(sparkSession)

    // 将 DataFrame 中的所有列转换为字符串类型，并移除特殊字符
    val stringTypedDf = df.columns.foldLeft(df)((currentDf, colName) => {
      try {
        currentDf.withColumn(colName, regexp_replace(col(colName).cast("string"), "[\\n\\t\\r]", ""))
      } catch {
        case e: Exception =>
          println(s"Error converting column $colName to string: ${e.getMessage}")
          currentDf
      }
    })

    stringTypedDf.createOrReplaceTempView(table)

    var sqlquery = s"SELECT * FROM $table"
    var resDf = sparkSession.sql(sqlquery)
    resDf.show()

    // 获取 DataFrame 的 schema
    var schema = resDf.schema

    // Spark SQL 数据类型到 ClickHouse 数据类型的映射
    import org.apache.spark.sql.types._

    def sparkTypeToClickHouseType(sparkType: DataType): String = sparkType match {
      case IntegerType => "Int32"
      case LongType => "Int64"
      case DoubleType => "Float64"
      case BooleanType => "UInt8"
      case DateType => "Date"
      case TimestampType => "DateTime"
      case _ => "String"  // 默认转换为 String
    }

    // 构建 ClickHouse 建表语句
    val createTableStatement = s"""
                                  |CREATE TABLE IF NOT EXISTS $clickhouseTable (
                                  |  ${schema.fields.map(f => s"${f.name} ${sparkTypeToClickHouseType(f.dataType)}").mkString(", ")}
                                  |) ENGINE = MergeTree()
                                  |ORDER BY ${schema.fields.head.name}
    """.stripMargin

    // 输出建表语句
    println(createTableStatement)

    // 在 ClickHouse 中创建表
    val clickhouseConnection = DriverManager.getConnection(clickhouseUrl, clickhouseUser, clickhousePassword)
    val clickhouseStatement = clickhouseConnection.createStatement()
    clickhouseStatement.execute(createTableStatement)
    clickhouseConnection.close()

    // 写入数据到 ClickHouse
    resDf
      .write
      .format("jdbc")
      .mode("append")
      .option("url", clickhouseUrl)
      .option("fetchsize", "500000")
      .option("driver", "ru.yandex.clickhouse.ClickHouseDriver")
      .option("user", clickhouseUser)
      .option("password", clickhousePassword)
      .option("dbtable", clickhouseTable)
      .save()

    // 关闭 SparkSession
    sparkSession.stop()
  }
}
