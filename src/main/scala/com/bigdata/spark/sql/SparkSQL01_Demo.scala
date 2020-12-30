package com.bigdata.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

object SparkSQL01_Demo {
  def main(args: Array[String]): Unit = {

    // SparkSQL

    // SparkConf
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SQL")

    // SparkContext
    // SparkSession
    // 创建SparkSQL环境对象
    val spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

    // 读取数据 构建DataFrame
    val frame: DataFrame = spark.read.json("in/user.json")

    // 展示数据
    frame.show()

    // 释放资源
    spark.stop()

  }
}
