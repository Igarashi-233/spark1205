package com.bigdata.spark.core.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object spark08_action_fold {
  def main(args: Array[String]): Unit = {

    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Action")

    val sc = new SparkContext(config)

    val listRDD: RDD[Int] = sc.makeRDD(1 to 10, 2)

    listRDD.glom().collect().foreach(x => {
      println(x.mkString(","))
    })

    //aggregate的简化操作
    //初始值在分区间和分区内都会累加
    val result: Int = listRDD.fold(10)(_ + _)

    println(result)

  }
}
