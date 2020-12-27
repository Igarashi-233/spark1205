package com.bigdata.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

//自定义分区其
object Spark27_Oper26 {
  def main(args: Array[String]): Unit = {

    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")

    val sc = new SparkContext(config)

    val listRDD: RDD[(Int, String)] = sc.makeRDD(Array((1, "a"), (1, "d"), (2, "b"), (3, "c")))

    listRDD.collect().foreach(t => {
      println(t)
    })

    val mapValueRDD: RDD[(Int, String)] = listRDD.mapValues(_ + "233")

    mapValueRDD.collect().foreach(t => {
      println(t)
    })

  }
}
