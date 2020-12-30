package com.bigdata.spark.core.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object spark06_action_takeOrdered {
  def main(args: Array[String]): Unit = {

    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Action")

    val sc = new SparkContext(config)

    val listRDD: RDD[Int] = sc.makeRDD(Array(2, 5, 4, 6, 8, 3))

    listRDD.takeOrdered(3).foreach(println)

  }
}
