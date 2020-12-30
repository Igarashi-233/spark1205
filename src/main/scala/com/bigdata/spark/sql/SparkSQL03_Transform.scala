package com.bigdata.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object SparkSQL03_Transform {
  def main(args: Array[String]): Unit = {

    // SparkSQL
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SQL")

    val spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

    val frame: DataFrame = spark.read.json("in/user.json")

    //创建RDD
    val rdd: RDD[(Int, String, Int)] = spark.sparkContext.makeRDD(List((1, "zhangsan", 20), (2, "lisi", 21), (3, "wangwu", 22)))

    //转换为DF
    // 在转换之前需要引入隐式转换规则 这里spark不是包名的含义 是SparkSession对象的名字
    import spark.implicits._
    val df: DataFrame = rdd.toDF("id", "name", "age")

    //转换为DS
    val ds: Dataset[User] = df.as[User]

    //转换为DF
    val df1: DataFrame = ds.toDF()

    //转换为RDD
    val rdd1: RDD[Row] = df1.rdd

    rdd1.foreach(row => {
      // 获取数据时可以通过索引访问数据
      println(row.getInt(0))
    })

    spark.stop()

  }
}

case class User(id: Int, name: String, age: Int) {

}