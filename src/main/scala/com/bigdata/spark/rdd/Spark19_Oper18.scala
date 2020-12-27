package com.bigdata.spark.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark19_Oper18 {
  def main(args: Array[String]): Unit = {

    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")

    val sc = new SparkContext(config)

    val listRDD: RDD[Int] = sc.makeRDD(Array(1, 2, 3), 3)

    //拉链时要保证分区数量和分区内部数据数量一致
    val listRDD2: RDD[String] = sc.makeRDD(Array("a", "b", "c", "d"), 4)

    val zipRDD: RDD[(Int, String)] = listRDD.zip(listRDD2)

    println(zipRDD.collect().mkString(","))

  }
}
