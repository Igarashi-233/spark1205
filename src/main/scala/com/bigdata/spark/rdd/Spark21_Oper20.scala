package com.bigdata.spark.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, Partitioner, SparkConf, SparkContext}

//自定义分区其
object Spark21_Oper20 {
  def main(args: Array[String]): Unit = {

    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")

    val sc = new SparkContext(config)

    val listRDD: RDD[String] = sc.makeRDD(Array("one", "two", "three", "four", "one", "four", "one"))

    val mapRDD: RDD[(String, Int)] = listRDD.map(x => {
      (x, 1)
    })

    // (three,CompactBuffer(1)),(two,CompactBuffer(1)),(four,CompactBuffer(1, 1)),(one,CompactBuffer(1, 1, 1))
    val groupByKeyRDD: RDD[(String, Iterable[Int])] = mapRDD.groupByKey()

    val mapByKeyRDD: RDD[(String, Int)] = groupByKeyRDD.map(t => {
      (t._1, t._2.sum)
    })

    println(mapByKeyRDD.collect().mkString(","))

  }
}
