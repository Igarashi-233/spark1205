package com.bigdata.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

// 有状态数据统计
object SparkStreaming06_Window {
  def main(args: Array[String]): Unit = {

    //Scala窗口
    val ints = List(1, 2, 3, 4, 5, 6)
    val intses: Iterator[List[Int]] = ints.sliding(3, 3)
    for (elem <- intses) {
      println(elem.mkString(","))
    }


    //Spark窗口
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Window")
    val streamingContext = new StreamingContext(sparkConf, Seconds(3))

    // 从Kafka采集数据
    val kafkaDStream: ReceiverInputDStream[(String, String)] = KafkaUtils.createStream(
      streamingContext,
      "hadoop102:2181",
      "IGARASHI",
      Map("IGARASHI" -> 3)
    )

    // 窗口大小为采集周期的整数倍 ,滑动步长也是采集周期的整数倍
    val windowDStream: DStream[(String, String)] = kafkaDStream.window(Seconds(9), Seconds(3))

    // 将采集的数据进行分解(扁平化)
    val wordsDStream: DStream[String] = windowDStream.flatMap(t => {
      t._2.split(" ")
    })

    // 转变数据结构 方便统计分析
    val mapDStream: DStream[(String, Int)] = wordsDStream.map(x => {
      (x, 1)
    })

    // 将转换结构后的数据进行聚合处理
    val wordToSumDStream: DStream[(String, Int)] = mapDStream.reduceByKey(_ + _)

    wordToSumDStream.print()

    streamingContext.start()
    streamingContext.awaitTermination()


  }
}