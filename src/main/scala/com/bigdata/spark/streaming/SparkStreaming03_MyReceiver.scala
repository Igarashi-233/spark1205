package com.bigdata.spark.streaming

import java.io.{BufferedReader, InputStreamReader}
import java.net.Socket

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.streaming.{Seconds, StreamingContext}

// 自定义采集器
object SparkStreaming03_MyReceiver {
  def main(args: Array[String]): Unit = {

    //使用SparkStreaming完成WordCount

    // Spark配置对象
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Streaming")

    // 实时数据分析环境对象
    // 采集周期 以指定时间为周期采集实时数据
    val streamingContext = new StreamingContext(sparkConf, Seconds(5))

    // 从指定端口采集数据
    val receiverDStream: ReceiverInputDStream[String] = streamingContext.receiverStream(new MyReceiver("hadoop102", 9999))

    // 将采集的数据进行分解(扁平化)
    val wordsDStream: DStream[String] = receiverDStream.flatMap(line => {
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

// 声明采集器
class MyReceiver(host: String, port: Int) extends Receiver[String](StorageLevel.MEMORY_ONLY) {

  var sokect: Socket = _

  def receive(): Unit = {

    val reader = new BufferedReader(new InputStreamReader(sokect.getInputStream, "UTF-8"))

    val line: String = reader.readLine()
    while (line != null) {

      if ("END".equals(line)) {
        return
      } else {
        //将采集的数据存储到采集器内部进行转换
        this.store(line)
      }

    }
  }

  override def onStart(): Unit = {

    sokect = new Socket(host, port)

    new Thread(new Runnable {
      override def run(): Unit = {
        receive()
      }
    }).start()

  }

  override def onStop(): Unit = {
    if (sokect != null) {
      sokect.close()
      sokect = null
    }
  }

}