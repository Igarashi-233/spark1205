package com.bigdata.spark.core.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark04_Oper3 {
  def main(args: Array[String]): Unit = {

    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")

    val sc = new SparkContext(config)

    val listRDD: RDD[Int] = sc.makeRDD(1 to 13, 2)

    val tupleRDD: RDD[(Int, String)] = listRDD.mapPartitionsWithIndex {
      case (num, datas) =>
        datas.map(data => {
          (data, "分区号: " + num)
        })
    }

    tupleRDD.collect().foreach(println)

  }
}
