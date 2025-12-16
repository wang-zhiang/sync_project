package sqlservertockutil

import org.apache.spark.sql.{DataFrame, SparkSession}

/*
* result.show() 就会输出到console里，就不会往下执行了
* */
   object  sqlservertohive {
  def main(args: Array[String]): Unit = {
    var  sqlServerUrl  ="jdbc:sqlserver://192.168.3.182;database=SyncWebSearchJD"
      var    sqlServerTable = "JD_ShopS4"
        var  sqlServerUser = "sa"
          var  sqlServerPassword = "smartpthdata"
    sqlservertohive (sqlServerUrl,sqlServerTable,sqlServerUser,sqlServerPassword)

  }


     def sqlservertohive (sqlServerUrl:String,sqlServerTable:String,sqlServerUser:String,sqlServerPassword:String): Unit  = {

      var a = "rescount"
       var b = "shopname"

      var spark = SparkSession.builder()

        .appName(this.getClass.getName)
        .master("local[*]")
        //.master("yarn")
        .getOrCreate()


       val frame = spark
         .read
         .format("jdbc")
         .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
         .option("url", sqlServerUrl)
         .option("dbtable", sqlServerTable)
         .option("user", sqlServerUser)
         .option("password", sqlServerPassword)
         .load()
       frame.createOrReplaceTempView("result")


      var result: DataFrame= spark.sql(s"select  ''code,''tablenameregex,'' batch,''targettype,$a  targetid, $b targetname ,''ym   from result ")


//      val sqlServerUrl = "jdbc:sqlserver://192.168.3.181:1433;DatabaseName=tempdb"
//      val sqlServerTable = "clickhouse_ceshi"
//      val sqlServerUser = "sa"
//      val sqlServerPassword = "smartpthdata"
//
//      //同步之前先删除sqlserver表里的数据
//      var bool = sqlserverUtil.isTableExist(sqlServerUrl,sqlServerUser,sqlServerPassword,sqlServerTable)
//      //如果sqlserver表不存在则建立
//      var   mode ="";
//      if(bool){
//
//         mode = "append"
//        //数据同步之前删除覆盖的数据
//        var query = "delete from " + sqlServerTable  + " " + cksqlserverUtil.wherereslut(Id)
//        print("删除覆盖的语句：" + query)
//        sqlserverUtil.executeSqlCommand(sqlServerUrl,sqlServerUser,sqlServerPassword,query)
//
//      }else{
//        mode = "overwrite"
//      }
//
//      var st = Util.getCurrentTimeFormatted
//      try {
//        mysqlutils.update("UPDATE sync_record_detail SET StartTime = ? where Id = ?", st, Id)
//        mysqlutils.update("UPDATE sync_record_detail SET Status = 1 where Id = ?", Id)
//      } catch {
//        case e: SQLException =>
//          print("mysql更新状态失败")
//          e.printStackTrace()
//          throw e
//      }

//       val driver = "org.apache.hive.jdbc.HiveDriver"
//       val url = "jdbc:hive2://192.168.5.104:10000"
//       val username = "smartpath"
//       val password = "cl@1109"


       //这里应该有个trycathch 失败的原因可以写在里面
      result
        .write
        .format("jdbc")
        .mode("append")
        .option("driver","org.apache.hive.jdbc.HiveDriver")
        .option("url","jdbc:hive2://192.168.5.104:10000/ods")
        .option("dbtable","spider_target_pool")
        .option("user","smartpath")
        .option("password","cl@1109")
         .option("fetchsize","1000")
        .save()


//      try {
//        var ft = Util.getCurrentTimeFormatted
//        mysqlutils.update("UPDATE sync_record_detail SET FinishTime = ? where Id = ?", ft, Id)
//        mysqlutils.update("UPDATE sync_record_detail SET Status = 2 where Id = ?", Id)
//        System.out.println("此次同步结束")
//      } catch {
//        case e: SQLException =>
//          print("mysql更新状态失败")
//          e.printStackTrace()
//          throw e
//
//      }

      spark.stop()
    }


  }


