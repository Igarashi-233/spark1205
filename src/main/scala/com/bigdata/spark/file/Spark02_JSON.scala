package com.bigdata.spark.file

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.parsing.json.JSON

object Spark02_JSON {
  def main(args: Array[String]): Unit = {

    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("File")

    val sc = new SparkContext(config)

    val json: RDD[String] = sc.textFile("in/user.json")

    val result: RDD[Option[Any]] = json.map(JSON.parseFull)

    result.foreach(println)

    sc.stop()

  }
}
