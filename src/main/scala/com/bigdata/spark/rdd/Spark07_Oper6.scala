package com.bigdata.spark.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark07_Oper6 {
  def main(args: Array[String]): Unit = {

    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")

    val sc = new SparkContext(config)

    //生成数据 按照制定规则进行分组
    val listRDD: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))

    //分组后的数据行程对偶元组(K-V)  K表示分组的key V表示分组的数据集合
    val groupByRDD: RDD[(Int, Iterable[Int])] = listRDD.groupBy(i => {
      i % 2
    })

    groupByRDD.collect().foreach(println)

  }
}
