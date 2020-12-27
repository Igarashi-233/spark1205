package com.bigdata.spark.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, Partitioner, SparkConf, SparkContext}

//自定义分区其
object Spark20_Oper19_2 {
  def main(args: Array[String]): Unit = {

    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")

    val sc = new SparkContext(config)

    val listRDD: RDD[(Int, String)] = sc.makeRDD(Array((1, "aaa"), (2, "bbb"), (3, "ccc"), (4, "ddd")), 4)

    val MyPartitionRDD: RDD[(Int, String)] = listRDD.partitionBy(new MyPartitioner(4))

    MyPartitionRDD.saveAsTextFile("output")

  }
}

//声明分区其
class MyPartitioner(partitions: Int) extends Partitioner {
  override def numPartitions: Int = {

    partitions
  }

  override def getPartition(key: Any): Int = {
    1
  }
}