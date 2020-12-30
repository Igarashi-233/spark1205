package com.bigdata.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object SparkSQL04_Transform1 {
  def main(args: Array[String]): Unit = {

    // SparkSQL
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SQL")

    val spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

    import spark.implicits._

    //创建RDD
    val rdd: RDD[(Int, String, Int)] = spark.sparkContext.makeRDD(List((1, "zhangsan", 20), (2, "lisi", 21), (3, "wangwu", 22)))

    // RDD -> DataSet
    val userRDD: RDD[User] = rdd.map {
      case (id, name, age) =>
        User(id, name, age)
    }

    val userDS: Dataset[User] = userRDD.toDS()

    val rdd1: RDD[User] = userDS.rdd

    rdd1.foreach(println)

    spark.stop()

  }
}