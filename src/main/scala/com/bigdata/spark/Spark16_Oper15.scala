package com.bigdata.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark16_Oper15 {
  def main(args: Array[String]): Unit = {

    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")

    val sc = new SparkContext(config)

    val listRDD: RDD[Int] = sc.makeRDD(3 to 8)

    val listRDD2: RDD[Int] = sc.makeRDD(1 to 6)

    val substractRDD: RDD[Int] = listRDD.subtract(listRDD2)

    // 7,8
    println(substractRDD.collect().mkString(","))

  }
}
