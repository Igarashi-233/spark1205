package com.bigdata.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, DoubleType, LongType, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

object SparkSQL06_diyUDAF {
  def main(args: Array[String]): Unit = {

    // SparkSQL
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SQL")

    val spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

    import spark.implicits._

    val frame: DataFrame = spark.read.json("in/user.json")

    frame.createTempView("xxx")

    //创建自定义聚合函数对象
    val udaf: MyAgeAvgFunction = new MyAgeAvgFunction

    // 注册聚合函数
    spark.udf.register("avgAge", udaf)

    //使用
    spark.sql("select avgAge(age) as age from xxx").show()

    spark.stop()

  }
}

//声明用户的自定义聚合函数
// 1. 继承UserDefinedAggregateFunction
// 2. 实现方法
class MyAgeAvgFunction extends UserDefinedAggregateFunction {

  // 输入数据结构
  override def inputSchema: StructType = {
    new StructType().add("age", LongType)
  }

  // 计算式的数据结构
  override def bufferSchema: StructType = {
    new StructType().add("sum", LongType).add("count", LongType)
  }

  // 返回值的数据类型
  override def dataType: DataType = {
    DoubleType
  }

  // 函数是否稳定
  override def deterministic: Boolean = true

  // 计算之前缓冲区的初始化
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    //sum
    buffer(0) = 0L
    //count
    buffer(1) = 0L
  }

  // 根据查询结果更新缓冲区数据
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    buffer(0) = buffer.getLong(0) + input.getLong(0)
    buffer(1) = buffer.getLong(1) + 1
  }

  // 将多个节点的缓冲区合并
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    // 根据结构顺序 1--sum
    buffer1(0) = buffer1.getLong(0) + buffer2.getLong(0)
    //           2--count
    buffer1(1) = buffer1.getLong(1) + buffer2.getLong(1)

  }

  // 计算最后的结果
  override def evaluate(buffer: Row): Any = buffer.getLong(0) / buffer.getLong(1).toDouble

}