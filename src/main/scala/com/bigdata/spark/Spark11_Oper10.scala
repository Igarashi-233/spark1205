package com.bigdata.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark11_Oper10 {
  def main(args: Array[String]): Unit = {

    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")

    val sc = new SparkContext(config)

    val listRDD: RDD[Int] = sc.makeRDD(1 to 16, 4)

    //缩减分区数量
    println("缩减分区前的分区数量 = " + listRDD.partitions.length)

    val coalesceRDD: RDD[Int] = listRDD.coalesce(3)

    //可以简单理解为合并分区
    println("缩减分区后的分区数量 = " + coalesceRDD.partitions.length)

    coalesceRDD.saveAsTextFile("output")

  }
}
