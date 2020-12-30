package com.bigdata.spark.core.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object spark09_action_file {
  def main(args: Array[String]): Unit = {

    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Action")

    val sc = new SparkContext(config)

    val listRDD: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("c", 2), ("c", 3)))

    listRDD.saveAsTextFile("output1")
    listRDD.saveAsSequenceFile("output2")
    listRDD.saveAsObjectFile("output3")

  }
}
