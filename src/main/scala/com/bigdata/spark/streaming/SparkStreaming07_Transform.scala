package com.bigdata.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

// 有状态数据统计
object SparkStreaming07_Transform {
  def main(args: Array[String]): Unit = {

    //Spark窗口
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Window")
    val streamingContext = new StreamingContext(sparkConf, Seconds(3))

    val socketLineDStream: ReceiverInputDStream[String] = streamingContext.socketTextStream("hadoop102", 9999)

    //转换
    //TODO (Driver)
    socketLineDStream.map {
      case x =>
        //TODO 代码(Excutor)
        x
    }

    //TODO (Driver)
    socketLineDStream.transform {
      case rdd => {
        //TODO (Driver)
        rdd.map {
          //TODO  (Excutor)
          case x => {
            x
          }
        }
      }
    }

    socketLineDStream.foreachRDD(rdd => {
      rdd.foreach(println)
    })


    streamingContext.start()
    streamingContext.awaitTermination()


  }
}