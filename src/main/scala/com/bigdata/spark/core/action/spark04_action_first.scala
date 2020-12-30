package com.bigdata.spark.core.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object spark04_action_first {
  def main(args: Array[String]): Unit = {

    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Action")

    val sc = new SparkContext(config)

    val listRDD: RDD[Int] = sc.makeRDD(1 to 10)

    println(listRDD.first())

  }
}
