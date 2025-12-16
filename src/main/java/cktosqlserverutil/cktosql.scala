package cktosqlserverutil

import com.mysql.cj.util.SaslPrep.StringType
import org.apache.calcite.avatica.ColumnMetaData.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.{DataFrame, SparkSession, SaveMode}
import org.apache.spark.sql.types._

/*
引入了spark的依赖所以才能使用
* result.show() 就会输出到console里，就不会往下执行了
* */
object  cktosql {
  //val clickhouseUrl1 = "jdbc:clickhouse://192.168.5.110:8123"
  val clickhouseUrl1 = "jdbc:clickhouse://192.168.5.111:8123"
  val clickhouseUser = "default"
  val clickhousePassword = "smartpath"






  def cktosqlserver (clickhouseTable: String,clihouseDB:String,clickhousefile:String,sqlServerUrl:String,sqlServerTable:String,sqlServerUser:String,sqlServerPassword:String): Unit  = {
    var  clickhouseUrl = clickhouseUrl1  +  "/" +  clihouseDB ;


    var spark = SparkSession.builder()

      .appName(this.getClass.getName)
      //.master("local[*]")
      .master("local[4]") // 使用 4 个本地线程
      .config("spark.executor.memory", "4g") // 每个执行器 4 GB 内存
      //.master("yarn")
      .getOrCreate()






    var readDataDf: DataFrame = spark.read
      .format("jdbc")
      .option("url", clickhouseUrl)
      .option("fetchsize", "30000")
      .option("driver", "ru.yandex.clickhouse.ClickHouseDriver")
      .option("user", clickhouseUser)
      .option("password", clickhousePassword)
      .option("dbtable", clickhouseTable)
      .option("max_execution_time", "0")
      .load()


    readDataDf.createOrReplaceTempView("result")
    print(clickhousefile)
    var result1: DataFrame= spark.sql(clickhousefile)


//           import org.apache.spark.sql.functions._
//           //注意这一行，里面0和1的位置是故意颠倒的，但是不颠倒输出到sqlserver里不正确，很奇怪
//           val result = result1.withColumn("isdel", when(col("isdel") === "false", 1).otherwise(0))
//           .withColumn("iscommit", when(col("iscommit") === "false", 0).otherwise(1))

   // import org.apache.spark.sql.types._





    //这里应该有个trycathch 失败的原因可以写在里面
    result1
      .write
      .format("jdbc")
      .mode("append")
      .option("driver","com.microsoft.sqlserver.jdbc.SQLServerDriver")
      .option("url",sqlServerUrl)
      .option("batchsize", "30000")
      .option("dbtable",sqlServerTable)
      .option("user",sqlServerUser)
      .option("password",sqlServerPassword)
      //.option("createTableColumnTypes", result.schema.map(field => s"${field.name} varchar(500)").mkString(","))
      .save()




    spark.stop()
  }
}


