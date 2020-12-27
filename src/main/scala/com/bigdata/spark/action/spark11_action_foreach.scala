package com.bigdata.spark.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object spark11_action_foreach {
  def main(args: Array[String]): Unit = {

    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Action")

    val sc = new SparkContext(config)

    val listRDD: RDD[Int] = sc.makeRDD(1 to 5, 2)

    listRDD.glom().collect().foreach(x => {
      println(x.mkString(","))
    })

    listRDD.foreach(println)

  }
}
