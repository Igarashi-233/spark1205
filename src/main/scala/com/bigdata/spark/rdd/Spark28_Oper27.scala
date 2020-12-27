package com.bigdata.spark.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

//自定义分区其
object Spark28_Oper27 {
  def main(args: Array[String]): Unit = {

    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")

    val sc = new SparkContext(config)

    val listRDD: RDD[(Int, String)] = sc.parallelize(Array((1, "a"), (2, "b"), (3, "c")))
    val listRDD2: RDD[(Int, Int)] = sc.parallelize(Array((1, 4), (2, 5), (3, 6)))

    listRDD.collect().foreach(t => {
      println(t)
    })

    listRDD2.collect().foreach(t => {
      println(t)
    })

    listRDD.join(listRDD2).collect().foreach(t => {
      println(t)
    })

  }
}
