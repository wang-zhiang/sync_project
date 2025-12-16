package elm_mt_filter

import org.apache.spark.sql.{DataFrame, SparkSession}

/*
引入了spark的依赖所以才能使用
* result.show() 就会输出到console里，就不会往下执行了
*
* 注意事项： 往sqlserver同步，ck的字段可以少，sqlserver的可以多，只要ck的字段在sqlservers上能找到对应的
*
* */
object  cktosqlserver {
  val clickhouseUrl1 = "jdbc:clickhouse://hadoop110:8123"
  val clickhouseUser = "default"
  val clickhousePassword = "smartpath"








  def cktosqlserver (clickhouseTable: String,clihouseDB:String,clickhousefile:String,sqlServerUrl:String,sqlServerTable:String,sqlServerUser:String,sqlServerPassword:String): Unit  = {
    var  clickhouseUrl = clickhouseUrl1  +  "/" +  clihouseDB


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
      .load()


    readDataDf.createOrReplaceTempView("result")
    print(clickhousefile)
    var result1: DataFrame= spark.sql(clickhousefile)


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
      .save()




    spark.stop()
  }
}


