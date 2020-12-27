package com.bigdata.spark.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark10_Oper9 {
  def main(args: Array[String]): Unit = {

    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")

    val sc = new SparkContext(config)

    val listRDD: RDD[Int] = sc.makeRDD(List(1, 7, 9, 6, 5, 2, 3, 1, 1, 5, 9))

    //    val distinctRDD: RDD[Int] = listRDD.distinct()
    val distinctRDD: RDD[Int] = listRDD.distinct(2)

    // distinct可以对数据进行去重 去重后数据减少 于是可以改变默认分区数量
    //    distinctRDD.collect().foreach(println)
    distinctRDD.saveAsTextFile("output")


  }
}
