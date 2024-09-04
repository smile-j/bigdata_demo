package com.demo.bigdata.spark_sreaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkStreaming07_OutPut1 {


  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("spark_streaming")
    val streamingContext: StreamingContext = new StreamingContext(sparkConf,Seconds(3))

    streamingContext.checkpoint("cp")

    //获取数据
    val lines: ReceiverInputDStream[String] = streamingContext.socketTextStream("localhost",9999)

    val wordToOne = lines.map(((_,1)))


    /**
      * reduceByKeyAndWindow:当前窗口范围比较大，但是滑动幅度比较小
      *   那么采用增加数据和删除数据的方法，无需重复计算
      */
    val windowDS = wordToOne.reduceByKeyAndWindow(
      (x:Int,y:Int)=>x+y
      , (x:Int,y:Int)=>x-y
      ,Seconds(9)
      ,Seconds(3))

    //SparkStreaming没有输出操作，会抛出错误
    //foreachRDD不会出现时间戳
    windowDS.foreachRDD(rdd=>{

    })


    //由于SparkStreaming采集器是长期执行的任务，所以不能直接关闭
    //如果main方法执行完毕，应用程序也会自动结束
//    streamingContext.stop()
//    //1.启动采集器
    streamingContext.start()
//    //2.等待采集器的关闭
    streamingContext.awaitTermination()


  }

}

