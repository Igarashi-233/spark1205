package com.bigdata.spark.core.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

//自定义分区其
object Spark25_Oper24 {
  def main(args: Array[String]): Unit = {

    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")

    val sc = new SparkContext(config)

    val listRDD: RDD[(String, Int)] = sc.makeRDD(Array(("a", 88), ("b", 95), ("a", 91), ("b", 93), ("a", 95), ("b", 98))
      , 2)

    listRDD.glom().collect().foreach(array => {
      println(array.mkString(","))
    })

    val combineRDD: RDD[(String, (Int, Int))] = listRDD.combineByKey(
      (_, 1),
      (acc: (Int, Int), v) => {
        (acc._1 + v, acc._2 + 1)
      },
      (acc1: (Int, Int), acc2: (Int, Int)) => {
        (acc1._1 + acc2._1, acc1._2 + acc2._2)
      }
    )

    //    listRDD.combineByKey(
    //      x => (x, 1),
    //      (acc: (Int, Int), v) => {
    //        (acc._1 + v, acc._2 + 1)
    //      },
    //      (acc1: (Int, Int), acc2: (Int, Int)) => {
    //        (acc1._1 + acc2._1, acc1._2 + acc2._2)
    //      })

    combineRDD.collect().foreach(array => {
      println(array)
    })

    val result: RDD[(String, Double)] = combineRDD.map {
      case (key, value) =>
        (key, value._1 / value._2.toDouble)
    }

    result.collect().foreach(t => {
      print(t)
    })

  }
}
