package hivesource

import mongodbtoclickhouse.mongotock.{clickhousePassword, clickhouseUser}
import org.apache.spark.sql.SparkSession
import org.apache.hadoop.fs.Path
import org.apache.hadoop.conf.Configuration
import org.apache.logging.log4j.{LogManager, Logger}

object connhive {

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
      .getOrCreate()

    // 应用 Hadoop 配置
    spark.sparkContext.hadoopConfiguration.addResource(hadoopConf)

    // 手动添加条件
      val whereresult = "  where pt_ym = '202510'"
    //val whereresult = "  where pt_ym >= '202101' and pt_ym <=  '202408' "
    val df = spark.sql(s"select * from  dwd.$hivetable" + whereresult)
    df.show()

    // 定义需要转换的字段名映射（Hive中的小写名 -> ClickHouse中的大写名），并指定其数据类型为Float32
    val columnMap = Map(
      "ldl_price" -> "LDL_Price",
      "ldl_qty" -> "LDL_Qty",
      "s4_price" -> "S4_Price",
      "s4_qty" -> "S4_Qty",
      "s3_price" -> "S3_Price",
      "s3_qty" -> "S3_Qty",
      "sp_price" -> "SP_Price",
      "sp_qty" -> "SP_Qty",
      "s5_price" -> "S5_Price",
      "s5_qty" -> "S5_Qty"
    )

    // 检查并替换 DataFrame 中的列名
    val renamedDF = df.columns.foldLeft(df) { (tempDF, colName) =>
      val lowerColName = colName.toLowerCase
      if (columnMap.contains(lowerColName)) {
        tempDF.withColumnRenamed(colName, columnMap(lowerColName))
      } else {
        tempDF
      }
    }

    var schema = renamedDF.schema

    // Spark SQL 数据类型到 ClickHouse 数据类型的映射，特定字段设为Float32
    import org.apache.spark.sql.types._

    def sparkTypeToClickHouseType(sparkType: DataType, fieldName: String): String = {
      if (columnMap.values.toList.contains(fieldName)) {
        "Float32" // 新添加的特定字段设为 Float32
      } else {
        sparkType match {
          case IntegerType => "Int32"
          case LongType => "Int64"
          case DoubleType => "Float64"
          case BooleanType => "UInt8"
          case DateType => "Date"
          case TimestampType => "DateTime"
          case _ => "String" // 默认转换为 String
        }
      }
    }

    // 构建 ClickHouse 建表语句
    val table_local = s"""$hivetable""" + "_local"
    var createTableStatement_local =
      s"""
  CREATE TABLE IF NOT EXISTS dwd.$hivetable""" +
        s"""_local  on cluster test_ck_cluster(
    ${schema.fields.map(f => s"${f.name} ${sparkTypeToClickHouseType(f.dataType, f.name)}").mkString(", ")}
  ) ENGINE = MergeTree()  PARTITION BY (pt_ym)
  ORDER BY ${schema.fields.head.name}
"""
    cksqlserverUtil.ckEXECUTE(createTableStatement_local, "dwd", hivetable)

    var createTableStatement =
      s"""
  CREATE TABLE IF NOT EXISTS dwd.$hivetable on cluster test_ck_cluster (
    ${schema.fields.map(f => s"${f.name} ${sparkTypeToClickHouseType(f.dataType, f.name)}").mkString(", ")}
  ) ENGINE = Distributed('test_ck_cluster', 'dwd', '$table_local' , rand())"""

    cksqlserverUtil.ckEXECUTE(createTableStatement, "dwd", hivetable)
    print("建表成功")
    // 输出建表语句
    println(createTableStatement_local)
    println(createTableStatement)

    // 插入前先根据条件删除数据
    val ckdatabasetable = "dwd." + hivetable + "_local"
    val stringsql = s"alter table $ckdatabasetable ON CLUSTER test_ck_cluster delete" + whereresult

    import java.sql.SQLException
    try {
      cksqlserverUtil.execute(stringsql)
      print("根据条件删除部分数据" + ckdatabasetable + "成功")
    } catch {
      case e: SQLException =>
        print("覆盖数据失败: " + e.getMessage)
    }

    print("删除语句为：" + stringsql)

    val clickhouseUrl1 = "jdbc:clickhouse://hadoop110:8123/dwd"
    val clickhouseUser = "default"
    val clickhousePassword = "smartpath"
    renamedDF
      .write
      .format("jdbc")
      .mode("append")
      .option("url", clickhouseUrl1)
      .option("fetchsize", "5000")
      .option("driver", "ru.yandex.clickhouse.ClickHouseDriver")
      .option("user", clickhouseUser)
      .option("password", clickhousePassword)
      .option("dbtable", hivetable)
      .save()

    spark.stop()
  }
}
