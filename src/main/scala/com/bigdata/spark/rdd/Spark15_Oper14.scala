package com.bigdata.spark.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark15_Oper14 {
  def main(args: Array[String]): Unit = {

    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")

    val sc = new SparkContext(config)

    val listRDD: RDD[Int] = sc.makeRDD(1 to 5)

    val listRDD2: RDD[Int] = sc.makeRDD(5 to 10)

    // 1,2,3,4,5,5,6,7,8,9,10
    val unionRdd: RDD[Int] = listRDD.union(listRDD2)

    println(unionRdd.collect().mkString(","))


  }
}
