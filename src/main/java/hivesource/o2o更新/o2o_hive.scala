package hivesource.o2o更新

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession


object o2o_hive {


  def gethive(hivetable: String): Unit = {

    System.setProperty("log4j.configuration", "D:\\wzza\\develop\\idea_project\\ceshi\\src\\main\\resources\\log4j2.xml")
    // 创建 Hadoop Configuration
    val hadoopConf = new Configuration()
    hadoopConf.addResource(new Path("D:\\wzza\\develop\\idea_project\\ceshi\\src\\main\\resources\\core-site.xml"))
    hadoopConf.addResource(new Path("D:\\wzza\\develop\\idea_project\\ceshi\\src\\main\\resources\\hdfs-site.xml"))
    hadoopConf.addResource(new Path("D:\\wzza\\develop\\idea_project\\ceshi\\src\\main\\resources\\yarn-site.xml"))
    hadoopConf.addResource(new Path("D:\\wzza\\develop\\idea_project\\ceshi\\src\\main\\resources\\hive-site.xml"))
    hadoopConf.addResource(new Path("D:\\wzza\\develop\\idea_project\\ceshi\\src\\main\\resources\\spark-defaults.xml"))


    // 创建 SparkSession


    var spark = SparkSession.builder()
      .appName("Hive Access")
      .master("local[*]")
      .enableHiveSupport()
      .config("spark.sql.parquet.enableVectorizedReader", "true")
      .config("spark.sql.inMemoryColumnarStorage.batchSize", "10000")
      .config("spark.sql.shuffle.partitions", "200") // 根据数据量调整
      .config("spark.executor.instances", "5")
      .config("spark.executor.memory", "4g")
      .config("spark.executor.cores", "4")
      .config("spark.driver.memory", "4g")
      .config("spark.driver.maxResultSize", "2g")
      .config("spark.kryoserializer.buffer.max", "512m")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.default.parallelism", "100")
      .config("spark.network.timeout", "800s")
      .config("spark.sql.broadcastTimeout", "1200")
      .config("spark.ui.enabled", "true")
      .getOrCreate()

    // 应用 Hadoop 配置


    spark.sparkContext.hadoopConfiguration.addResource(hadoopConf)
    // import spark.implicits._
    //手动添加条件
    //val whereresult = "  where pt_ym >= '202301' and pt_ym <= '202311' "
    val whereresult = "  where 1 = 1"
    val df = spark.sql(s"select * from  ods.$hivetable" + whereresult)

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
    val table_local =
      s"""$hivetable""" + "_local"
    var createTableStatement_local =
      s"""
  CREATE TABLE IF NOT EXISTS ods.$hivetable""" +
        s"""_local  on cluster test_ck_cluster(
    ${schema.fields.map(f => s"${f.name} ${sparkTypeToClickHouseType(f.dataType)}").mkString(", ")}
  ) ENGINE = MergeTree()  PARTITION BY (pt_ym)
  ORDER BY ${schema.fields.head.name}
"""
    hivesource.cksqlserverUtil.ckEXECUTE(createTableStatement_local,"dwd",hivetable)

    var createTableStatement =
      s"""
  CREATE TABLE IF NOT EXISTS ods.$hivetable on cluster test_ck_cluster (
    ${schema.fields.map(f => s"${f.name} ${sparkTypeToClickHouseType(f.dataType)}").mkString(", ")}
  ) ENGINE = Distributed('test_ck_cluster', 'ods', '$table_local' , rand())"""

    hivesource.cksqlserverUtil.ckEXECUTE(createTableStatement,"ods",hivetable)
    print("建表成功")
    // 输出建表语句
    println(createTableStatement_local)
    println(createTableStatement)

    //插入前先根据条件删除whereresutl
    val  ckdatabasetable = "ods." + hivetable + "_local"
    val  stringsql = s"alter table $ckdatabasetable ON CLUSTER test_ck_cluster delete" + whereresult
    //val  stringsql = s"truncate table $ckdatabasetable ON CLUSTER test_ck_cluster"

    import java.sql.SQLException
    try {
      hivesource.cksqlserverUtil.execute(stringsql)
      // 如果没有异常发生，则执行成功
      print("根据条件删除部分数据" + ckdatabasetable + "成功")
    } catch {
      case e: SQLException =>

        print("覆盖数据失败: " + e.getMessage)

    }

    print("删除语句为：" + stringsql)

    val clickhouseUrl1 = "jdbc:clickhouse://hadoop104:8123/ods"
    val clickhouseUser = "default"
    val clickhousePassword = "smartpath"
    df
        .write
        .format("jdbc")
        .mode("append")
        .option("url", clickhouseUrl1)
        .option("fetchsize", "10000")
        .option("driver", "ru.yandex.clickhouse.ClickHouseDriver")
        .option("user", clickhouseUser)
        .option("password", clickhousePassword)
        .option("dbtable", hivetable)
        .save()
      spark.stop()

  }
}
