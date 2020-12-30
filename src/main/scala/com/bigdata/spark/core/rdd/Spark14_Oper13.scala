package com.bigdata.spark.core.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark14_Oper13 {
  def main(args: Array[String]): Unit = {

    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")

    val sc = new SparkContext(config)

    val listRDD: RDD[Int] = sc.makeRDD(List(2, 1, 3, 9, 4, 6), 4)

    //根据自身大小排序 升序
    println(listRDD.sortBy(x => x).collect().mkString(","))

    //降序
    println(listRDD.sortBy(x => x, ascending = false).collect().mkString(","))

  }
}
