package com.bigdata.spark.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

//自定义分区其
object Spark23_Oper22 {
  def main(args: Array[String]): Unit = {

    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")

    val sc = new SparkContext(config)

    val listRDD: RDD[(String, Int)] = sc.makeRDD(List(("a", 3), ("a", 2), ("c", 4), ("b", 3), ("c", 6), ("c", 8)), 2)

    listRDD.glom().collect().foreach(array => {
      println(array.mkString(","))
    })

    //初始值只会在分区内进行累加
    val aggregateRDD: RDD[(String, Int)] = listRDD.aggregateByKey(0)(List(_, _).max, _ + _)

    aggregateRDD.collect().foreach(array => {
      println(array)
    })

  }
}
