package com.bigdata.spark.core.sharedata

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark03_BroadCast {
  def main(args: Array[String]): Unit = {

    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("DataBase")

    val sc = new SparkContext(config)

    val dataRDD: RDD[(Int, String)] = sc.makeRDD(List((1, "a"), (2, "b"), (3, "c")))

    /**
     *
     * joinRDD:
     * (3,(c,3))
     * (2,(b,2))
     * (1,(a,1))
     * val dataRDD2: RDD[(Int, Int)] = sc.makeRDD(List((1, 1), (2, 2), (3, 3)))
     *
     * val joinRDD: RDD[(Int, (String, Int))] = dataRDD.join(dataRDD2)
     *
     * joinRDD.foreach(println)
     */

    val list = List((1, 1), (2, 2), (3, 3))

    //广播变量减少数据传递
    //1.构建广播变量
    val dataRDD2: Broadcast[List[(Int, Int)]] = sc.broadcast(list)

    //2.使用广播变量
    val resultRDD: RDD[(Int, (String, Any))] = dataRDD.map {
      case (key, value) =>
        var v2: Any = null
        for (t <- dataRDD2.value) {
          if (key == t._1) {
            v2 = t._2
          }
        }
        (key, (value, v2))
    }
    resultRDD.foreach(println)

  }
}
