package com.bigdata.spark.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark09_Oper8 {
  def main(args: Array[String]): Unit = {

    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")

    val sc = new SparkContext(config)

    val listRDD: RDD[Int] = sc.makeRDD(1 to 10)

    //从指定集合中进行抽样处理 根据不同算法进行抽样
    val sampleRDD: RDD[Int] = listRDD.sample(withReplacement = false, 0.4, 1)

    sampleRDD.collect().foreach(println)


  }
}
