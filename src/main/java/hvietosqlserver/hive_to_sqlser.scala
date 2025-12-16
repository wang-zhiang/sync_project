package hvietosqlserver

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession


object hive_to_sqlser {


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
      //.config("spark.sql.warehouse.dir", "/user/hive/warehouse/dwd")
      .master("local[*]")
      .enableHiveSupport()
      .config("spark.executor.memory", "4g")
      .config("spark.driver.memory", "4g")
      .config("spark.default.parallelism", "200")
      .config("spark.sql.shuffle.partitions", "200")
      .getOrCreate()

    // 应用 Hadoop 配置


    spark.sparkContext.hadoopConfiguration.addResource(hadoopConf)
    // import spark.implicits._
    //手动添加条件
    //val whereresult = "  where pt_ym >= '202001' and pt_ym <= '202012' "

    val whereresult = "  where 1 = 1 "
    val df = spark.sql(s"select * from  ods.$hivetable" + whereresult)
   // val df = spark.sql(s"select * from  dwd.$hivetable where pt_ym >= '202001'")
    df.show()



//    val clickhouseUrl1 = "jdbc:clickhouse://hadoop110:8123/dwd"
//    val clickhouseUser = "default"
//    val clickhousePassword = "smartpath"
    val  sqlServerUrl = "jdbc:sqlserver://192.168.4.51;DatabaseName=Taobao_trading";

    val sqlServerUsername = "sa"
    val sqlServerPassword = "smartpathdata"
    val  sqlservertable  ="taobaoduibi_res_2508"

    df
      .write
      .format("jdbc")
      .mode("overwrite")
      .option("driver","com.microsoft.sqlserver.jdbc.SQLServerDriver")
      .option("url",sqlServerUrl)
      .option("batchsize", "40000")
      .option("dbtable",sqlservertable)
      .option("user",sqlServerUsername)
      .option("password",sqlServerPassword)
      //.option("createTableColumnTypes", result.schema.map(field => s"${field.name} varchar(500)").mkString(","))
      .save()


    spark.stop()

  }
}
