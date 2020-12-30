package com.bigdata.spark.core.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

//检查点
object Spark30_CheckPoint {
  def main(args: Array[String]): Unit = {

    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")

    val sc = new SparkContext(config)
    //设定检查点的保存目录
    sc.setCheckpointDir("cp")

    val listRDD: RDD[Int] = sc.parallelize(List(1, 2, 3, 4))

    val mapRDD: RDD[(Int, Int)] = listRDD.map((_, 1))

    mapRDD.checkpoint()

    val reduceKeyRDD: RDD[(Int, Int)] = mapRDD.reduceByKey(_ + _)

    reduceKeyRDD.foreach(println)

    println(reduceKeyRDD.toDebugString)

    reduceKeyRDD.foreach(println)

  }
}
