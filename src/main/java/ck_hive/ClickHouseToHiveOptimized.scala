package ck_hive

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.hadoop.fs.Path
import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.types._ // 导入Spark SQL类型库

object ClickHouseToHiveOptimized {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "smartpath")
    // ... 其他配置保持不变 ...
    // 设置日志配置文件
    System.setProperty("log4j.configuration", "D:\\wzza\\develop\\idea_project\\ceshi\\src\\main\\resources\\log4j2.xml")

    // 创建 Hadoop Configuration
    val hadoopConf = new Configuration()
    hadoopConf.addResource(new Path("D:\\wzza\\develop\\idea_project\\ceshi\\src\\main\\resources\\core-site.xml"))
    hadoopConf.addResource(new Path("D:\\wzza\\develop\\idea_project\\ceshi\\src\\main\\resources\\hdfs-site.xml"))
    hadoopConf.addResource(new Path("D:\\wzza\\develop\\idea_project\\ceshi\\src\\main\\resources\\yarn-site.xml"))
    hadoopConf.addResource(new Path("D:\\wzza\\develop\\idea_project\\ceshi\\src\\main\\resources\\hive-site.xml"))
    hadoopConf.addResource(new Path("D:\\wzza\\develop\\idea_project\\ceshi\\src\\main\\resources\\spark-defaults.xml"))

    val spark = SparkSession.builder()
      .appName("ClickHouse to Hive Optimized")
      .master("local[*]")
      .config("spark.sql.shuffle.partitions", "50")
      .config("spark.sql.parquet.compression.codec", "snappy")
      .config("spark.executor.memory", "4g")
      .config("spark.driver.memory", "2g")
      .enableHiveSupport()
      .getOrCreate()

    // 应用 Hadoop 配置
    spark.sparkContext.hadoopConfiguration.addResource(hadoopConf)

    // ClickHouse 配置
    val clickhouseUrl = "jdbc:clickhouse://hadoop104:8123"
    val clickhouseDatabase = "dwd"
    val clickhouseTable = "taobao_2508_temp"
    val clickhouseUser = "default"
    val clickhousePassword = "smartpath"

    // Hive 配置
    val hiveDatabase = "ods"
    val hiveTable = "taobao_2508_new2"


    // 优化后的读取配置
    val df = spark.read
      .format("jdbc")
      .option("url", s"$clickhouseUrl/$clickhouseDatabase")
      .option("query", s"select *, rowNumberInAllBlocks() as spark_partition_id from $clickhouseTable")
      .option("user", clickhouseUser)
      .option("password", clickhousePassword)
      .option("driver", "ru.yandex.clickhouse.ClickHouseDriver")
      .option("numPartitions", "10")
      .option("partitionColumn", "spark_partition_id")
      .option("lowerBound", "0")
      .option("upperBound", "1024")
      .load()
      .drop("spark_partition_id")

    // 优化写入前处理
    val optimizedDF = df.repartition(10)

    // 启用批量写入配置
    spark.conf.set("hive.exec.dynamic.partition.mode", "nonstrict")
    spark.conf.set("hive.vectorized.execution.enabled", "true")

    df.show()

    // 检查 Hive 表是否存在
    spark.sql(s"CREATE DATABASE IF NOT EXISTS $hiveDatabase")

    val tableExists = spark.catalog.tableExists(hiveDatabase, hiveTable)

    if (!tableExists) {
      // 根据 ClickHouse 表的 schema 创建 Hive 表
      val schema = df.schema

      def sparkTypeToHiveType(sparkType: DataType): String = sparkType match {
        case IntegerType => "INT"
        case LongType => "BIGINT"
        case DoubleType => "DOUBLE"
        case BooleanType => "BOOLEAN"
        case DateType => "DATE"
        case TimestampType => "TIMESTAMP"
        case _ => "STRING" // 默认转换为 STRING
      }

      val createTableSQL = s"""
        CREATE TABLE IF NOT EXISTS $hiveDatabase.$hiveTable (
          ${schema.fields.map(f => s"${f.name} ${sparkTypeToHiveType(f.dataType)}").mkString(", ")}
        ) STORED AS PARQUET
      """

      try {
        spark.sql(createTableSQL)
        println(s"Created Hive table: $hiveDatabase.$hiveTable")
      } catch {
        case e: Exception => println(s"Failed to create Hive table: $hiveDatabase.$hiveTable. Error: ${e.getMessage}")
      }
    }

    // 优化写入操作
    optimizedDF.write
      .mode(SaveMode.Append)
      .format("hive")
      .insertInto(s"$hiveDatabase.$hiveTable")

    spark.stop()
  }
}