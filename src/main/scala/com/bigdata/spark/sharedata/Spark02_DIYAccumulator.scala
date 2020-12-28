package com.bigdata.spark.sharedata

import java.util

import org.apache.spark.rdd.RDD
import org.apache.spark.util.{AccumulatorV2, LongAccumulator}
import org.apache.spark.{SparkConf, SparkContext}

//自定义累加器
object Spark02_DIYAccumulator {
  def main(args: Array[String]): Unit = {

    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("DataBase")

    val sc = new SparkContext(config)

    val dataRDD: RDD[String] = sc.makeRDD(List("Hadoop", "Hive", "Scala", "Java", "Hbase", "Spark"), 2)

    //TODO 声明累加器
    val wordAccumulator = new WordAccumulator

    //TODO 注册累加器
    sc.register(wordAccumulator)

    dataRDD.foreach {
      word => {
        wordAccumulator.add(word)
      }
    }
    //TODO 获取累加器的值
    println("sum= " + wordAccumulator.value)

    sc.stop()
  }
}

//继承父类
//重写方法
class WordAccumulator extends AccumulatorV2[String, util.ArrayList[String]] {

  val list = new util.ArrayList[String]()

  //当前累加器是否为初始化状态
  override def isZero: Boolean = list.isEmpty

  //复制累加器对象
  override def copy(): AccumulatorV2[String, util.ArrayList[String]] = {
    new WordAccumulator()
  }

  //重置累加器对象
  override def reset(): Unit = list.clear()

  //向累加器中添加数据
  override def add(v: String): Unit = {

    if (v.contains("a")) {
      list.add(v)
    }

  }

  //合并累加器
  override def merge(other: AccumulatorV2[String, util.ArrayList[String]]): Unit = {
    list.addAll(other.value)
  }

  //获取累加器结果
  override def value: util.ArrayList[String] = list

}