package ck_hive

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.hadoop.fs.Path
import org.apache.hadoop.conf.Configuration

object sync {

  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "smartpath")

    val sourceConf = Map(
      "host" -> "hadoop110",
      "port" -> "8123",
      "user" -> "default",
      "password" -> "smartpath",
      "database" -> "dwd",
      "table" -> "source4_ldl"
    )

    val destConf = Map(
      "host" -> "192.168.5.111",
      "port" -> "8123",
      "user" -> "default",
      "password" -> "smartpath",
      "database" -> "dwd",
      "table" -> "source4_ldl_ceshi"
    )

    val whereCondition = "WHERE pt_ym='202502'"

    val hadoopConf = new Configuration()
    hadoopConf.addResource(new Path("D:\\wzza\\develop\\idea_project\\ceshi\\src\\main\\resources\\core-site.xml"))
    hadoopConf.addResource(new Path("D:\\wzza\\develop\\idea_project\\ceshi\\src\\main\\resources\\hdfs-site.xml"))

    val spark = SparkSession.builder()
      .appName("ClickHouse Sync Fixed")
      .master("local[*]")
      .config("spark.sql.shuffle.partitions", "20")
      .getOrCreate()

    try {
      // 修复点1：构建符合ClickHouse语法的查询语句
      val baseQuery = {
        val select = s"SELECT *, 1 AS spark_partition_id FROM ${sourceConf("database")}.${sourceConf("table")}"
        if (whereCondition.nonEmpty) s"$select $whereCondition" else select
      }

      println(baseQuery)  // 用于调试输出查询语句

      // 修复点2：修改查询格式，避免子查询的AS别名问题
      val query = s"SELECT * FROM ($baseQuery) AS tmp"

      // 修复点3：使用query选项替代dbtable，去掉 partitionColumn 选项
      val df = spark.read
        .format("jdbc")
        .option("url", s"jdbc:clickhouse://${sourceConf("host")}:${sourceConf("port")}")
        .option("query", query) // 直接使用完整查询语句
        .option("user", sourceConf("user"))
        .option("password", sourceConf("password"))
        .option("driver", "ru.yandex.clickhouse.ClickHouseDriver")
        .option("fetchsize", "100000")
        .load()
        .drop("spark_partition_id") // 移除伪列

      // 写入配置保持不变
      df.write
        .format("jdbc")
        .option("url", s"jdbc:clickhouse://${destConf("host")}:${destConf("port")}/${destConf("database")}")
        .option("dbtable", destConf("table"))
        .option("user", destConf("user"))
        .option("password", destConf("password"))
        .option("driver", "ru.yandex.clickhouse.ClickHouseDriver")
        .option("batchsize", "100000")
        .option("isolationLevel", "NONE")
        .mode(SaveMode.Append)
        .save()

      println("数据同步完成！")

    } catch {
      case e: Exception =>
        println(s"同步失败: ${e.getMessage}")
        e.printStackTrace()
    } finally {
      spark.stop()
    }
  }
}
