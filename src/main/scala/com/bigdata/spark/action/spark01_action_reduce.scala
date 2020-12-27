package com.bigdata.spark.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object spark01_action_reduce {
  def main(args: Array[String]): Unit = {

    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Action")

    val sc = new SparkContext(config)

    val listRDD: RDD[Int] = sc.makeRDD(1 to 10, 2)

    val result: Int = listRDD.reduce(_ + _)

    println(result)

    val listRDD2: RDD[(String, Int)] = sc.makeRDD(Array(("a", 1), ("a", 3), ("c", 3), ("d", 5)))

    val tuple: (String, Int) = listRDD2.reduce((x, y) => {
      (x._1 + y._1, x._2 + y._2)
    })

    println(tuple)

  }
}
