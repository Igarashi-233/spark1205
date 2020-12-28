package com.bigdata.spark.sharedata

import org.apache.spark.rdd.RDD
import org.apache.spark.util.LongAccumulator
import org.apache.spark.{SparkConf, SparkContext}

object Spark01_Accumulator {
  def main(args: Array[String]): Unit = {

    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("DataBase")

    val sc = new SparkContext(config)

    val dataRDD: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4), 2)
    //sum=10
    //    val sum: Int = dataRDD.reduce(_ + _)

    // sum=0  sum在Driver中 Executer下的foreach中执行后的sum没有返回Driver sum一直为0
    //    var sum = 0
    //    dataRDD.foreach(i => {
    //      sum += i
    //    })

    //累加器(只写)实现 sum=10
    //1.创建累加器对象
    val sum: LongAccumulator = sc.longAccumulator

    dataRDD.foreach {
      i => {
        sum.add(i)
      }
    }
    println(sum.value)

    sc.stop()
  }
}
