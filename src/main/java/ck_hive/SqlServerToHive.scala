package ck_hive



package ck_hive

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.hadoop.fs.Path
import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.types._ // 导入Spark SQL类型库

object SqlServerToHive {

  def main(args: Array[String]): Unit = {

    //解决了权限问题
    System.setProperty("HADOOP_USER_NAME", "smartpath")
    // 设置日志配置文件
    System.setProperty("log4j.configuration", "D:\\wzza\\develop\\idea_project\\ceshi\\src\\main\\resources\\log4j2.xml")

    // 创建 Hadoop Configuration
    val hadoopConf = new Configuration()
    hadoopConf.addResource(new Path("D:\\wzza\\develop\\idea_project\\ceshi\\src\\main\\resources\\core-site.xml"))
    hadoopConf.addResource(new Path("D:\\wzza\\develop\\idea_project\\ceshi\\src\\main\\resources\\hdfs-site.xml"))
    hadoopConf.addResource(new Path("D:\\wzza\\develop\\idea_project\\ceshi\\src\\main\\resources\\yarn-site.xml"))
    hadoopConf.addResource(new Path("D:\\wzza\\develop\\idea_project\\ceshi\\src\\main\\resources\\hive-site.xml"))
    hadoopConf.addResource(new Path("D:\\wzza\\develop\\idea_project\\ceshi\\src\\main\\resources\\spark-defaults.xml"))

    // 创建 SparkSession
    val spark = SparkSession.builder()
      .appName("SQL Server to Hive")
      .master("local[*]")
      .enableHiveSupport()
      .getOrCreate()

    // 应用 Hadoop 配置
    spark.sparkContext.hadoopConfiguration.addResource(hadoopConf)

    // SQL Server 配置
    val sqlServerUrl = "jdbc:sqlserver://192.168.4.36;databaseName=websearchc"
    val sqlServerTable = "jd_2406"
    val sqlServerUser = "CHH"
    val sqlServerPassword = "Y1v606"

    // Hive 配置
    val hiveDatabase = "dwd"
    val hiveTable = "jd_s4_2406"

    // 从 SQL Server 读取数据
    val df = spark.read
      .format("jdbc")
      .option("url", sqlServerUrl)
      .option("dbtable", sqlServerTable)
      .option("user", sqlServerUser)
      .option("password", sqlServerPassword)
      .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
      .load()

    df.show()

    // 检查 Hive 表是否存在


    val tableExists = spark.catalog.tableExists(hiveDatabase, hiveTable)

    if (!tableExists) {
      // 根据 SQL Server 表的 schema 创建 Hive 表
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

    // 将数据写入 Hive
    try {
      df.write
        .mode(SaveMode.Append) // 追加模式
        .insertInto(s"$hiveDatabase.$hiveTable")

      println(s"数据已成功从 SQL Server 同步到 Hive 表 $hiveDatabase.$hiveTable")
    } catch {
      case e: Exception => println(s"Failed to insert data into Hive table: $hiveDatabase.$hiveTable. Error: ${e.getMessage}")
    }

    spark.stop()
  }
}
