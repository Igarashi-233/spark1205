package com.bigdata.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

// 窗口操作
object SparkStreaming05_UpdateState {
  def main(args: Array[String]): Unit = {

    // Spark配置对象
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Streaming")

    // 实时数据分析环境对象
    // 采集周期 以指定时间为周期采集实时数据
    val streamingContext = new StreamingContext(sparkConf, Seconds(5))

    //保存数据状态 设定检查点路径
    streamingContext.sparkContext.setCheckpointDir("chkP")

    // 从Kafka采集数据
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
    val updateStateDStream: DStream[(String, Int)] = mapDStream.updateStateByKey {
      case (seq, buffer) =>
        val sum: Int = buffer.getOrElse(0) + seq.sum
        Option(sum)

    }

    // 打印
    updateStateDStream.print()

    // 不能停止采集功能
    // 启动采集器
    streamingContext.start()
    // Driver等待采集器执行
    streamingContext.awaitTermination()

  }
}