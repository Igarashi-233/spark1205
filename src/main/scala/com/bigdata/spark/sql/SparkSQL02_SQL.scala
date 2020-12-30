package com.bigdata.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

object SparkSQL02_SQL {
  def main(args: Array[String]): Unit = {

    // SparkSQL
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SQL")

    val spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

    val frame: DataFrame = spark.read.json("in/user.json")

    frame.createTempView("user")

    // SQL语法访问数据
    spark.sql("select * from user").show()

    spark.stop()

  }
}
