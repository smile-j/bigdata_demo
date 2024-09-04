package com.demo.bigdata.spark_sreaming

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable
import scala.util.Random

object SparkStreaming04_DIY {


  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("spark_streaming")
    val streamingContext: StreamingContext = new StreamingContext(sparkConf,Seconds(3))

    val messageDS: ReceiverInputDStream[String] = streamingContext.receiverStream(new MyReceiver())
    messageDS.print()


//    streamingContext.stop()
//    //1.启动采集器
    streamingContext.start()

//    //2.等待采集器的关闭
    streamingContext.awaitTermination()


  }

  /**
    * 自定义数据采集器
    *   1.继承Receiver ，定义泛型，传递参数
    *   2.重写方法
    *
    */
  class MyReceiver extends Receiver[String](StorageLevel.MEMORY_ONLY){

    private var flag:Boolean = true

    override def onStart(): Unit = {
      new Thread(()=>{
          while (true){
            val msg = "采集的数据为:"+new Random().nextInt(10).toString
            store(msg)
            Thread.sleep(500)
          }
      }).start()
    }

    override def onStop(): Unit = {
      flag = false
    }

  }
}

