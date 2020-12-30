package com.bigdata.spark.core.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

//自定义分区其
object Spark24_Oper23 {
  def main(args: Array[String]): Unit = {

    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")

    val sc = new SparkContext(config)

    val listRDD: RDD[(Int, Int)] = sc.makeRDD(List((1, 3), (1, 2), (1, 4), (2, 3), (3, 6), (3, 8)), 3)

    listRDD.glom().collect().foreach(array => {
      println(array.mkString(","))
    })

    //初始值在只会分区内都会累加
    val foldRDD: RDD[(Int, Int)] = listRDD.foldByKey(0)(_ + _)

    foldRDD.collect().foreach(array => {
      println(array)
    })

  }
}
