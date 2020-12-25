package com.bigdata.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

//自定义分区其
object Spark22_Oper21 {
  def main(args: Array[String]): Unit = {

    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")

    val sc = new SparkContext(config)

    val listRDD: RDD[String] = sc.makeRDD(Array("one", "two", "three", "four", "one", "four", "one"))

    val mapRDD: RDD[(String, Int)] = listRDD.map(array => {
      (array, 1)
    })

    val reduceKeyRDD: RDD[(String, Int)] = mapRDD.reduceByKey((key, num) => {
      key + num
    })

    println(reduceKeyRDD.collect().mkString(","))

  }
}
