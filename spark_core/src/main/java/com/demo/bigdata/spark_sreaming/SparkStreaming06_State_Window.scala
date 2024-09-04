package com.demo.bigdata.spark_sreaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkStreaming06_State_Window {


  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("spark_streaming")
    val streamingContext: StreamingContext = new StreamingContext(sparkConf,Seconds(3))



    //获取数据
    val lines: ReceiverInputDStream[String] = streamingContext.socketTextStream("localhost",9999)

    val wordToOne = lines.map(((_,1)))

    //窗口范围应该是采集周期的整数被
    //窗口可以滑动的，但是默认情况下，一个采集周期进行滑动
    //这样的话，可能数据重复计算，为了避免这种情况，可以改变滑动周期，第二个参数控制
    val windowDS = wordToOne.window(Seconds(6),Seconds(6))

    windowDS
      .reduceByKey((_+_))
      .print()


    //由于SparkStreaming采集器是长期执行的任务，所以不能直接关闭
    //如果main方法执行完毕，应用程序也会自动结束
//    streamingContext.stop()
//    //1.启动采集器
    streamingContext.start()
//    //2.等待采集器的关闭
    streamingContext.awaitTermination()


  }

}

