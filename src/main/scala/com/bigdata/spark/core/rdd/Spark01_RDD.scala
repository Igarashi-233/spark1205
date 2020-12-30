package com.bigdata.spark.core.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark01_RDD {
  def main(args: Array[String]): Unit = {

    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")

    val sc = new SparkContext(config)

    //创建RDD
    //1. 从内存中创建 makeRDD 底层实现是 parallelize
//    val listRDD: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))

    //自定义分区
    val listRDD: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4),2)
    //    val value: RDD[String] = sc.makeRDD(Array("1", "2", "3"))

    //2. 从内存中创建 parallelize
    val arrayRDD: RDD[Int] = sc.parallelize(Array(1, 2, 3, 4, 6, 7))

    //3. 从外部存储中创建  默认情况可以读取项目路径 也可以读取其他路径
    //文件读取数据默认为字符串
    //读取文件时分区数不一定是设置的分区数 取决于hadoop的分片规则
    val fileRDD: RDD[String] = sc.textFile("in",3)


//    listRDD.collect().foreach(x => {
//      println(x)
//    })
//
//    arrayRDD.collect().foreach(println)

    //将listRDD保存到文件中
    fileRDD.saveAsTextFile("output")


  }
}
