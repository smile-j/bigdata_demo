package com.demo.bigdata.spark_sreaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext, StreamingContextState}

object SparkStreaming09_Resume {


  def main(args: Array[String]): Unit = {

    val ssc = StreamingContext.getActiveOrCreate("cp", () => {
      val sparkConf = new SparkConf().setMaster("local[*]").setAppName("spark_streaming")
      val streamingContext: StreamingContext = new StreamingContext(sparkConf, Seconds(3))

      streamingContext.checkpoint("cp")

      //获取数据
      val lines: ReceiverInputDStream[String] = streamingContext.socketTextStream("localhost", 9999)

      val wordToOne = lines.map(((_, 1)))

      wordToOne.print()

      streamingContext
    })

    ssc.checkpoint("cp")

    ssc.start()

    ssc.awaitTermination()



  }

}

