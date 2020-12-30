package com.bigdata.spark.core.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark03_Oper2 {
  def main(args: Array[String]): Unit = {

    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")

    val sc = new SparkContext(config)

    val listRDD: RDD[Int] = sc.makeRDD(1 to 10)

    // mapPartitions可以对一个RDD分区中所有分区进行遍历
    val mapPartitionsRDD: RDD[Int] = listRDD.mapPartitions(datas => {
      datas.map(data => data * 2)
    })

    mapPartitionsRDD.collect().foreach(println)

  }
}
