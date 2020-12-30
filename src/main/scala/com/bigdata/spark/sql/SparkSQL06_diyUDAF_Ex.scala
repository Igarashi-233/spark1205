package com.bigdata.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, Dataset, Encoder, Encoders, SparkSession, TypedColumn}
import org.apache.spark.sql.expressions.Aggregator

object SparkSQL06_diyUDAF_Ex {
  def main(args: Array[String]): Unit = {

    // SparkSQL
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SQL")

    val spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

    import spark.implicits._

    val frame: DataFrame = spark.read.json("in/user.json")

    // 创建聚合函数对象
    val udaf = new MyAgeAvgClassFunction

    // 将聚合函数转换为查询列
    val avgCol: TypedColumn[UserData, Double] = udaf.toColumn.name("avgAge")

    val userDS: Dataset[UserData] = frame.as[UserData]

    // 应用
    userDS.select(avgCol).show()

    spark.stop()

  }
}

case class UserData(name: String, age: BigInt, damage: BigInt)

case class AvgBuffer(var sum: BigInt, var count: Int)

//声明用户的自定义聚合函数(强类型)
// 继承Aggregator 设定泛型
// 实现方法
class MyAgeAvgClassFunction extends Aggregator[UserData, AvgBuffer, Double] {

  // 初始化
  override def zero: AvgBuffer = {
    AvgBuffer(0, 0)
  }

  // 聚合数据
  override def reduce(b: AvgBuffer, a: UserData): AvgBuffer = {
    b.sum = b.sum + a.age
    b.count += 1

    b
  }

  // 缓冲区合并
  override def merge(b1: AvgBuffer, b2: AvgBuffer): AvgBuffer = {
    b1.sum = b1.sum + b2.sum
    b1.count = b1.count + b2.count

    b1
  }

  // 完成计算
  override def finish(reduction: AvgBuffer): Double = {
    reduction.sum.toDouble / reduction.count
  }

  override def bufferEncoder: Encoder[AvgBuffer] = Encoders.product

  override def outputEncoder: Encoder[Double] = Encoders.scalaDouble

}