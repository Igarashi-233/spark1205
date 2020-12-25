package com.bigdata.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark18_Oper17 {
  def main(args: Array[String]): Unit = {

    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")

    val sc = new SparkContext(config)

    val listRDD: RDD[Int] = sc.makeRDD(1 to 3)

    val listRDD2: RDD[Int] = sc.makeRDD(2 to 5)

    // (1,2),(1,3),(1,4),(1,5),(2,2),(2,3),(2,4),(2,5),(3,2),(3,3),(3,4),(3,5) 笛卡尔积
    val cartesianRDD: RDD[(Int, Int)] = listRDD.cartesian(listRDD2)

    println(cartesianRDD.collect().mkString(","))

  }
}
