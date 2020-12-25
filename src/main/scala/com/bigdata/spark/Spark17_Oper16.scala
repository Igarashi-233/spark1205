package com.bigdata.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark17_Oper16 {
  def main(args: Array[String]): Unit = {

    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")

    val sc = new SparkContext(config)

    val listRDD: RDD[Int] = sc.makeRDD(1 to 8)

    val listRDD2: RDD[Int] = sc.makeRDD(5 to 10)

    // 5,6,7,8
    val intersectionRDD: RDD[Int] = listRDD.intersection(listRDD2)

    println(intersectionRDD.collect().mkString(","))

  }
}
