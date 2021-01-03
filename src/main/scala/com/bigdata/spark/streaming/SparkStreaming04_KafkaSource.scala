package com.bigdata.spark.streaming

import java.io.{BufferedReader, InputStreamReader}
import java.net.Socket

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.streaming.{Seconds, StreamingContext}

// 自定义采集器
object SparkStreaming04_KafkaSource {
  def main(args: Array[String]): Unit = {

    //使用SparkStreaming完成WordCount

    // Spark配置对象
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Streaming")

    // 实时数据分析环境对象
    // 采集周期 以指定时间为周期采集实时数据
    val streamingContext = new StreamingContext(sparkConf, Seconds(5))

    // 从Kafka采集数据
    //    streamingContext.receiverStream(new MyReceiver("hadoop102", 9999))
    val kafkaDStream: ReceiverInputDStream[(String, String)] = KafkaUtils.createStream(
      streamingContext,
      "hadoop102:2181",
      "IGARASHI",
      Map("IGARASHI" -> 3)
    )

    // 将采集的数据进行分解(扁平化)
    val wordsDStream: DStream[String] = kafkaDStream.flatMap(t => {
      t._2.split(" ")
    })

    // 转变数据结构 方便统计分析
    val mapDStream: DStream[(String, Int)] = wordsDStream.map(x => {
      (x, 1)
    })

    // 将转换结构后的数据进行聚合处理
    val wordToSumDStream: DStream[(String, Int)] = mapDStream.reduceByKey(_ + _)

    // 打印
    wordToSumDStream.print()

    // 不能停止采集功能
    // 启动采集器
    streamingContext.start()
    // Driver等待采集器执行
    streamingContext.awaitTermination()


  }
}