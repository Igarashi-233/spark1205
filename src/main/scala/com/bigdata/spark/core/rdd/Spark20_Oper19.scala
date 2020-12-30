package com.bigdata.spark.core.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

object Spark20_Oper19 {
  def main(args: Array[String]): Unit = {

    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")

    val sc = new SparkContext(config)

    val listRDD: RDD[(Int, String)] = sc.makeRDD(Array((1, "aaa"), (2, "bbb"), (3, "ccc"), (4, "ddd")), 4)

    //重新分区前的分区数
    println(listRDD.partitions.length)

    val partitionByRDD: RDD[(Int, String)] = listRDD.partitionBy(new HashPartitioner(2))

    //重新分区后的分区数
    println(partitionByRDD.partitions.length)

    partitionByRDD.glom().collect().foreach(array => {
      println(array.mkString(","))
    })

  }
}
