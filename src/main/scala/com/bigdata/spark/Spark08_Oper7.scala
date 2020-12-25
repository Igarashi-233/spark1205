package com.bigdata.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark08_Oper7 {
  def main(args: Array[String]): Unit = {

    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")

    val sc = new SparkContext(config)

    //生成数据 按照制定规则进行过滤
    val listRDD: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))

    //余数为0的数据留下
    val filterRDD: RDD[Int] = listRDD.filter(i => {
      i % 2 == 0
    })


    filterRDD.collect().foreach(println)

  }
}
