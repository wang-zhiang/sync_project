
import org.apache.spark.sql.{SparkSession, SaveMode}

object a {
  def main(args: Array[String]): Unit = {

    // 创建 SparkSession
    val spark = SparkSession
      .builder()
      .appName("HiveExample")
      .master("local[*]")
      //.config("hive.metastore.uris","thrift://hadoop104:9083")
      .enableHiveSupport()
      .getOrCreate()



    // 查询Hive表
    //spark.sql("select * from tmp.sp_1 ").show
   spark.sql("show databases").show



    // 关闭 SparkSession
    spark.stop()
  }
}
