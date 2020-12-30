package com.bigdata.spark.core.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark13_Oper12 {
  def main(args: Array[String]): Unit = {

    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")

    val sc = new SparkContext(config)

    val listRDD: RDD[Int] = sc.makeRDD(1 to 16, 4)

    //    listRDD.coalesce()

    //    repartition实际上就是调用coalesce 并且默认设置shuffle

    //    listRDD.repartition()
  }
}
