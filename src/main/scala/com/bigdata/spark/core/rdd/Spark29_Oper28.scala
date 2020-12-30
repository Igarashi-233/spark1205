package com.bigdata.spark.core.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

//自定义分区其
object Spark29_Oper28 {
  def main(args: Array[String]): Unit = {

    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")

    val sc = new SparkContext(config)

    val rdd: RDD[(Int, String)] = sc.parallelize(Array((1, "a"), (2, "b"), (3, "c")))

    val rdd1: RDD[(Int, Int)] = sc.parallelize(Array((1, 4), (2, 5), (3, 6)))

    rdd.cogroup(rdd1).collect().foreach(t => {
      println(t)
    })
  }
}
