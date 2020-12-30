package com.bigdata.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object SparkSQL05_diyUDF {
  def main(args: Array[String]): Unit = {

    // SparkSQL
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SQL")

    val spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

    val frame: DataFrame = spark.read.json("in/user.json")

    frame.show()

    val addName: UserDefinedFunction = spark.udf.register("addName", (x: String) => {
      "Name:" + x
    })

    val addAge: UserDefinedFunction = spark.udf.register("addAge", (x: Int) => {
      "Age:" + x
    })

    frame.createTempView("xxx")

    spark.sql("select addName(name) as name, addAge(age) as age from xxx").show()

    spark.stop()

  }
}