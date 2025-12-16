package mysqltohive



import org.apache.spark.sql.{DataFrame, SparkSession}

/*
* result.show() 就会输出到console里，就不会往下执行了
* seatunnel不支持8.几版本的mysql问题解决
*
* */
object  mysqltosqlserver {
  def main(args: Array[String]): Unit = {
    var  sqlServerUrl  ="jdbc:sqlserver://192.168.3.181;database=tempdb"
    var    sqlServerTable = "elm_shop_2012"
    var  sqlServerUser = "sa"
    var  sqlServerPassword = "smartpthdata"




    var spark = SparkSession.builder()

      .appName(this.getClass.getName)
      .master("local[*]")
      .getOrCreate()

    val frame = spark
      .read
      .format("jdbc")
      .option("driver", "com.mysql.cj.jdbc.Driver")
      .option("url", "jdbc:mysql://plmysql.mysql.polardb.rds.aliyuncs.com/elm_real?serverTimezone=GMT%2b8")
      .option("dbtable", "shop_elm")
      .option("user", "sibo_data")
      .option("password", "sibo_data1")
      .load()




    //这里应该有个trycathch 失败的原因可以写在里面
    frame
      .write
      .format("jdbc")
      .mode("overwrite")
      .option("driver","com.microsoft.sqlserver.jdbc.SQLServerDriver")
      .option("url",sqlServerUrl)
      .option("dbtable",sqlServerTable)
      .option("user",sqlServerUser)
      .option("password",sqlServerPassword)
      .save()




    spark.stop()
  }


}


