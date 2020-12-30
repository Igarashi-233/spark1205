package com.bigdata.spark.core.file

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object Spark01_Text {
  def main(args: Array[String]): Unit = {

    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("File")

    val sc = new SparkContext(config)

    val value: RDD[String] = sc.textFile("in")

    value.saveAsTextFile("output")
  }
}
