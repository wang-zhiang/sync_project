package mongodbtoclickhouse.douyin

import java.sql.{Connection, DriverManager, ResultSet}
import com.mongodb.spark.MongoSpark
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{ArrayType, StringType}
import org.slf4j.LoggerFactory

object usually_tiktok {
  val clickhouseUser = "default"
  val clickhousePassword = "smartpath"
  val logger = LoggerFactory.getLogger(getClass)

  def getClickHouseSchema(clickhouseUrl: String, clickhouseTable: String): Map[String, String] = {
    val connection: Connection = DriverManager.getConnection(clickhouseUrl, clickhouseUser, clickhousePassword)
    val statement = connection.createStatement()
    val resultSet: ResultSet = statement.executeQuery(s"DESCRIBE TABLE " + clickhouseTable)

    var schema = Map[String, String]()

    while (resultSet.next()) {
      val columnName = resultSet.getString(1)
      val dataType = resultSet.getString(2)
      schema += (columnName -> dataType)
    }

    resultSet.close()
    statement.close()
    connection.close()

    schema
  }

  def preprocessToString(df: DataFrame): DataFrame = {
    df.schema.fields.foldLeft(df) { case (tempDf, field) =>
      try {
        tempDf.withColumn(field.name, col(field.name).cast(StringType))
      } catch {
        case e: Exception =>
          logger.error(s"Error converting field '${field.name}' to StringType: ${e.getMessage}", e)
          tempDf
      }
    }
  }

  def mongotock(clickhouseTable: String, clickhouseUrl: String, mongodb: String, mongodbbase: String, server: String): Unit = {
    val table = mongodb
    val mongobase = mongodbbase
    val dc = mongobase + "." + table

    // 打印日志，显示正在同步的MongoDB表
    logger.info(s"正在同步MongoDB表: $dc")

    val sparkConf: SparkConf = new SparkConf()
      .setMaster("local[*]")
      .setAppName(this.getClass.getName)
      .set("spark.mongodb.input.uri", s"mongodb://smartpath:smartpthdata@192.168.5.101:27017,192.168.5.102:27017,192.168.5.103:27017/$dc")
      .set("spark.executor.memory", "4g")  // 每个Executor分配4GB内存
      .set("spark.driver.memory", "2g")    // Driver程序分配2GB内存
      .set("spark.executor.instances", "4")  // 启动4个Executor实例
      .set("spark.default.parallelism", "5")  // 设置并行度为5

      //这个参数不设置，这个表 tiktok_live_20240716_20240716 就会有问题
      .set("spark.mongodb.input.sampleSize", "100000")  // ，基于整个集合推断模式

    val sparkSession: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

    // 加载MongoDB数据
    var df: DataFrame = MongoSpark.load(sparkSession)

    // 将所有字段转换为String类型
    df = preprocessToString(df)

    // 添加对每行数据的日志记录
//    df.foreach(row => {
//      logger.info(s"Processing row: $row")
//    })

    // 处理数组类型的字段，将其转换为字符串并保留数组的格式
    df.columns.foreach { colName =>
      try {
        if (df.schema(colName).dataType.isInstanceOf[ArrayType]) {
          df = df.withColumn(colName, expr(s"concat('[', array_join($colName, ', '), ']')")) // 将数组转换为字符串并保留原始格式
        }

        if (colName.contains("占比")) {
          val newColName = colName.replace("占比", "zb")
          df = df.withColumnRenamed(colName, newColName)
        }
      } catch {
        case e: Exception =>
          logger.error(s"Error processing column '$colName': ${e.getMessage}", e)
      }
    }

    // 检查是否有createdate或tdate字段并生成pt_ym字段
    val finalDf = try {
      if (df.columns.contains("tdate")) {
        df.withColumn("pt_ym", date_format(col("tdate"), "yyyyMM"))
      } else if (df.columns.contains("createdate")) {
        df.withColumn("pt_ym", date_format(col("createdate"), "yyyyMM"))
      } else if (df.columns.contains("create_time")) {
        df.withColumn("pt_ym", date_format(col("create_time"), "yyyyMM"))
      } else {
        // 从集合名称中提取日期部分（假设格式为 tiktok_live_20230814_20230814）
        val datePattern = ".*_(\\d{8})$".r
        val extractedDate = mongodb match {
          case datePattern(date) => date.substring(0, 6) // 提取日期的前六位
          case _ => throw new RuntimeException("缺少日期字段: 没有createdate或tdate字段，并且集合名称不包含日期")
        }
        df.withColumn("pt_ym", lit(extractedDate))
      }
    } catch {
      case e: Exception =>
        logger.error(s"Error generating 'pt_ym' field: ${e.getMessage}", e)
        throw e
    }

    // 根据 server 字段的内容决定如何重命名 _id
    val dfWithId = try {
      if (server.contains("飞瓜本地生活")) {
        finalDf.withColumn("Mongo_id", regexp_extract(col("_id").cast("string"), "([0-9a-fA-F]{24})", 1)).drop("_id")
      } else {
        finalDf.withColumn("id", regexp_extract(col("_id").cast("string"), "([0-9a-fA-F]{24})", 1)).drop("_id")
      }
    } catch {
      case e: Exception =>
        logger.error(s"Error processing '_id' field: ${e.getMessage}", e)
        throw e
    }

    // 获取ClickHouse表的字段和数据类型
    val ckSchema = getClickHouseSchema(clickhouseUrl, clickhouseTable)

    // 排除不需要动态补齐的手动添加的字段
    val manualFields = Set("server", "source_table", "pt_ym", "pt_channel")

    // 动态补全缺失字段，并设置默认值，并且去除 MongoDB 表字段中的特殊字符
    val finalDfWithSchema = ckSchema.foldLeft(dfWithId) { case (tempDf, (field, dataType)) =>
      var updatedDf = tempDf
      try {
        // 处理 MongoDB 字段缺失的情况，确保 ClickHouse 表结构一致
        if (!dfWithId.columns.contains(field) && !manualFields.contains(field)) {
          updatedDf = updatedDf.withColumn(field, lit(""))
        }

        // 对已存在的字段进行 null 处理，防止转换错误
        if (dfWithId.columns.contains(field)) {
          dataType match {
            case "String" => updatedDf = updatedDf.withColumn(field, coalesce(col(field).cast(StringType), lit("")))
            case "Int32" | "Int64" | "UInt32" | "UInt64" => updatedDf = updatedDf.withColumn(field, coalesce(col(field).cast("int"), lit(0)))
            case "Float32" | "Float64" => updatedDf = updatedDf.withColumn(field, coalesce(col(field).cast("float"), lit(0.0)))
            case _ => updatedDf = updatedDf.withColumn(field, coalesce(col(field).cast(StringType), lit("")))  // 如果是其他类型，默认为空字符串
          }
        }

        // 去除特殊字符 \n, \t, \r
        if (!manualFields.contains(field)) {
          updatedDf = updatedDf.withColumn(field, regexp_replace(col(field), "[\\n\\t\\r]", ""))
        }
      } catch {
        case e: Exception =>
          logger.error(s"Error processing field '$field' in row ${tempDf}: ${e.getMessage}", e)
      }
      updatedDf
    }

    // 添加手动声明的字段
    val extendedDf = finalDfWithSchema
      .withColumn("server", lit(server))  // 添加字段 server
      .withColumn("source_table", lit(mongodb))  // 添加字段 source_table
      .withColumn("pt_channel", lit("tiktok"))  // 添加字段 pt_channel

    extendedDf.createOrReplaceTempView(table)

    val sqlquery = s"SELECT * FROM $table"
    val resDf = sparkSession.sql(sqlquery)
    resDf.show()

    // 写入数据到 ClickHouse
    try {
      resDf
        .selectExpr(ckSchema.keys.toSeq.map(f => s"coalesce($f, '') as $f"): _*)  // 确保最终DataFrame包含ClickHouse中的所有字段
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
    } catch {
      case e: Exception =>
        logger.error(s"Error writing to ClickHouse: ${e.getMessage}", e)
        throw e
    }

    // 关闭 SparkSession
    sparkSession.stop()
  }
}
