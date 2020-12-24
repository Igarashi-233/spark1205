package com.bigdata.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object WordCount {
  def main(args: Array[String]): Unit = {

    //使用开发工具完成Spark WordCount开发

    //创建sparkConf对象  设定部署环境
    val config = new SparkConf().setMaster("local[*]").setAppName("WordCount")

    //创建 sc
    val sc = new SparkContext(config)

    //    println(sc)

    //读取文件  将文件内容一行一行的读取
    val lines: RDD[String] = sc.textFile("file:///opt/module/spark/in")

    //将每一行数据分解为单个单词
    val words: RDD[String] = lines.flatMap(_.split(" "))

    //结构转换
    val wordToOne: RDD[(String, Int)] = words.map(w => {
      (w, 1)
    })

    //对转换后的数据进行分组聚合
    val wordToSum = wordToOne.reduceByKey(_ + _)

    //将统计结果采集后打印到控制台
    val result = wordToSum.collect()

    println(result.mkString(","))

  }
}
