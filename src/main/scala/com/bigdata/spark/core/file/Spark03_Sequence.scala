package com.bigdata.spark.core.file

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark03_Sequence {
  def main(args: Array[String]): Unit = {

    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("File")

    val sc = new SparkContext(config)

    val rdd: RDD[(Int, Int)] = sc.parallelize(Array((1, 2), (3, 4), (5, 6)))

    //    rdd.saveAsSequenceFile("output/seqFile")

    val result: RDD[(Int, Int)] = sc.sequenceFile[Int, Int]("output/seqFile")

    result.collect().foreach(println)

  }
}
