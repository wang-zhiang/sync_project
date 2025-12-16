package test


import org.apache.spark.{SparkConf, SparkContext}

object ceshi {
  def main(args: Array[String]): Unit = {
    // 创建Spark配置和上下文
    val conf = new SparkConf().setAppName("Coalesce Example").setMaster("local[*]")
    val sc = new SparkContext(conf)

    // 创建一个包含6个元素的RDD，并将其分成3个分区
    val rdd = sc.parallelize(Seq(1, 2, 3, 4, 5, 6), 3)

    // 打印原始RDD的分区及其内容
    println("Original RDD Partitions:")
    rdd.mapPartitionsWithIndex((index, iter) => iter.map(x => s"Partition $index: $x")).collect().foreach(println)

    // 使用coalesce将分区数量从3减少到2
    val coalescedRDD = rdd.coalesce(2)

    // 打印coalesce后的RDD的分区及其内容
    println("\nCoalesced RDD Partitions:")
    coalescedRDD.mapPartitionsWithIndex((index, iter) => iter.map(x => s"Partition $index: $x")).collect().foreach(println)

    // 停止Spark上下文
    sc.stop()
  }
}
