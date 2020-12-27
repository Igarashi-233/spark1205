package com.bigdata.sparkrealSample

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 时间戳，省份，城市，用户，广告
 * 样本如下：
 * 1516609143867 6 7 64 16
 * 1516609143869 9 4 75 18
 * 1516609143869 1 7 87 12
 * .......
 */
//统计出每一个省份广告被点击次数的TOP3
object Practice {
  def main(args: Array[String]): Unit = {

    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Practice")

    val sc = new SparkContext(config)

    val lines: RDD[String] = sc.textFile("in/agent.log")

    val provinceAdToOne: RDD[((String, String), Int)] = lines.map(line => {
      val strings: Array[String] = line.split(" ")
      ((strings(1), strings(4)), 1)
    })

    //聚合方式一
    //    val provinceAdToSum: RDD[((String, String), Int)] = provinceAdToOne.reduceByKey((x, y) => {
    //      x + y
    //    })

    //聚合方式二
    //    val provinceAdToSum: RDD[((String, String), Int)] = provinceAdToOne.aggregateByKey(0)((x, y) => x + y, (x, y) => x + y)

    //聚合方式三
    //    val provinceAdToSum: RDD[((String, String), Int)] = provinceAdToOne.foldByKey(0)((x, y) => x + y)

    //聚合方式四
    val provinceAdToSum: RDD[((String, String), Int)] = provinceAdToOne.combineByKey(
      x => x,
      (acc: Int, v) => {
        acc + v
      },
      (acc1: Int, acc2: Int) => {
        acc1 + acc2
      }
    )

    val provinceToAdSum: RDD[(String, (String, Int))] = provinceAdToSum.map {
      case (key, value) =>
        (key._1, (key._2, value))
    }

    val provinceAdTop3: RDD[(String, List[(String, Int)])] = provinceToAdSum.groupByKey().mapValues(Iter => {
      Iter.toList.sortWith((x, y) => {
        x._2 > y._2
      }).take(3)
    })

    provinceAdTop3.collect().foreach(println)


  }
}
