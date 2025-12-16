import org.apache.spark.{SparkConf, SparkContext}

//测试预聚合的效果的
object test3 {


  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("MapSideAggregationExample").setMaster("local[*]")
    val sc = new SparkContext(conf)

    // 假设有一个RDD，其中每个元素是一个单词，并指定分区数
    val rdd = sc.parallelize(Seq("apple", "banana", "apple", "orange", "banana", "banana","apple", "banana", "apple", "orange", "banana", "banana"), numSlices = 3)

    // 输出 RDD 的分区数
    println(s"Number of partitions in the original RDD: ${rdd.getNumPartitions}")

    // Step 1: 将数据映射为 (word, 1) 的形式
    val wordPairs = rdd.map(word => (word, 1))

    // Step 2: 在每个分区内进行局部聚合
    val preAggregated = wordPairs.mapPartitionsWithIndex((index, iter) => {
      // 创建一个本地 Map 来存储局部聚合结果
      val localAggregation = scala.collection.mutable.Map[String, Int]()

      iter.foreach { case (word, count) =>
        localAggregation(word) = localAggregation.getOrElse(word, 0) + count
      }


      // 返回局部聚合结果
      localAggregation.iterator
    })

    // 在局部聚合后再查看每个分区的数据情况
    preAggregated.mapPartitionsWithIndex((index, iter) => {
      val partitionData = iter.toList
      println(s"Partition $index data after pre-aggregation: ${partitionData.mkString(", ")}")
      partitionData.iterator
    }).collect()

    // Step 3: 对预聚合结果进行全局聚合
    val finalAggregated = preAggregated.reduceByKey(_ + _)

    // 打印最终的聚合结果
    finalAggregated.collect().foreach(println)

    // 停止SparkContext
    sc.stop()
  }
}
