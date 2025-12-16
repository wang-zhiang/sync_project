package hivesource

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession

object util {

  def gethive (hivetable: String): Unit = {

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
      .getOrCreate()

    // 应用 Hadoop 配置


    spark.sparkContext.hadoopConfiguration.addResource(hadoopConf)
    // import spark.implicits._
    val df = spark.sql(s"DESCRIBE  dwd.$hivetable")

    // 提取第一列并转换为 List
    val columnNames = df.select("col_name").collect().toList

    // 使用逗号连接列名
    val concatenatedColumnNames = columnNames.mkString(",").toString().replace("[","").replace("]","")
    println(concatenatedColumnNames)

    spark.stop()
  }
}
