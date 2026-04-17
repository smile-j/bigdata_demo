package com.demo.bigdata.spark_sreaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext, StreamingContextState}

object SparkStreaming08_Close {


  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("spark_streaming")
    val streamingContext: StreamingContext = new StreamingContext(sparkConf,Seconds(3))

    streamingContext.checkpoint("cp")

    //获取数据
    val lines: ReceiverInputDStream[String] = streamingContext.socketTextStream("localhost",9999)

    val wordToOne = lines.map(((_,1)))

    wordToOne.print()


    streamingContext.start()

    //如果想要关闭采集器，那么需要创建新的线程
    //需要第三方状态来控制是否关闭
    new Thread(()=>{
      //优雅关闭
      //计算节点不在接受新的数据，而是将现有数据处理完毕，然后关闭
      //通过第三方状态判断

//      while (true){
//        //if 第三方状态 判断
//        if(true){
//          val state: StreamingContextState = streamingContext.getState()
//          if(state == StreamingContextState.ACTIVE){
//            streamingContext.stop(true,true)
//          }
//        }
//      Thread.sleep(5000)
//      }

      Thread.sleep(5000)
      val state: StreamingContextState = streamingContext.getState()
      if(state == StreamingContextState.ACTIVE){
        streamingContext.stop(true,true)
      }

      System.exit(0)

    }).start()

    streamingContext.awaitTermination()



  }

}

