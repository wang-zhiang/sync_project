package test

import org.apache.spark.{SparkConf, SparkContext}

object ceshi2 {
  def main(args: Array[String]): Unit = {
    // 创建Spark配置和上下文
    val conf = new SparkConf().setAppName("Coalesce Example").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val rdd = sc.parallelize(Seq(("a", 1), ("b", 2), ("c", 3), ("a", 4)), 3)
    rdd.mapPartitionsWithIndex((index, iter) => iter.map(x => s"Partition $index: $x")).collect().foreach(println)
    val partitionedRDD = rdd.partitionBy(new org.apache.spark.HashPartitioner(2))
    println(partitionedRDD.getNumPartitions)  // 输出: 2
    partitionedRDD.mapPartitionsWithIndex((index, iter) => iter.map(x => s"Partition $index: $x")).collect().foreach(println)


    // 停止Spark上下文
    sc.stop()
  }
}
