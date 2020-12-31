package com.bigdata.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkStreaming02_FileDataSource {
  def main(args: Array[String]): Unit = {

    //使用SparkStreaming完成WordCount

    // Spark配置对象
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Streaming")

    // 实时数据分析环境对象
    // 采集周期 以指定时间为周期采集实时数据
    val streamingContext = new StreamingContext(sparkConf, Seconds(5))

    // 从指定文件夹中采集数据
    val fileDStream: DStream[String] = streamingContext.textFileStream("test")

    // 将采集的数据进行分解(扁平化)
    val wordsDStream: DStream[String] = fileDStream.flatMap(line => {
      line.split(" ")
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
