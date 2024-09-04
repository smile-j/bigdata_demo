package com.demo.bigdata.spark_sreaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkStreaming06_State_Join {


  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("spark_streaming")
    val streamingContext: StreamingContext = new StreamingContext(sparkConf,Seconds(3))

    /**
      * join
      * 两个流之间的join需要两个流的批次大小一致，这样才能做到同时触发计算。计算过程就是
      * 对当前批次的两个流中各自的RDD进行join，与两个RDD的join效果相同。
      */

    //获取数据
    val data9999: ReceiverInputDStream[String] = streamingContext.socketTextStream("localhost",9999)
    val data8888: ReceiverInputDStream[String] = streamingContext.socketTextStream("localhost",8888)

    val map9999 = data9999.map((_,1))
    val map8888 = data8888.map((_,1))
    val joinDs: DStream[(String, (Int, Int))] = map9999.join(map8888)

    joinDs.print()

    //由于SparkStreaming采集器是长期执行的任务，所以不能直接关闭
    //如果main方法执行完毕，应用程序也会自动结束
//    streamingContext.stop()
//    //1.启动采集器
    streamingContext.start()
//    //2.等待采集器的关闭
    streamingContext.awaitTermination()


  }

}

