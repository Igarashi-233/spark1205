package com.bigdata.spark.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object spark07_action_aggregate {
  def main(args: Array[String]): Unit = {

    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Action")

    val sc = new SparkContext(config)

    val listRDD: RDD[Int] = sc.makeRDD(1 to 10, 2)

    listRDD.glom().collect().foreach(x => {
      println(x.mkString(","))
    })

    //初始值在分区间和分区内都会累加
    val result: Int = listRDD.aggregate(0)((x, y) => {
      x + y
    }, (x, y) => {
      x + y
    })

    println(result)

  }
}
