package com.demo.bigdata.spark_sreaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkStreaming06_State_Transform {


  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("spark_streaming")
    val streamingContext: StreamingContext = new StreamingContext(sparkConf,Seconds(3))

    /**
      * nc -l -p 9999
      *
      * nc localhost 9000 监听端口号
      */

    //获取数据
    val lines: ReceiverInputDStream[String] = streamingContext.socketTextStream("localhost",9999)

    //transform 方法可以将底层RDD获取到后进行操
    //1.DStream功能不完善
    //2.需要代码周期性执行

    //Code:Driver端
    val newDs: DStream[String] = lines.transform(
      rdd =>{
        rdd.map(
          //Code:Driver端,(周期性执行)
          str=>{
            //Code:Executor端
            str
        })
      }
    )
    //Code:Driver端
    val newDs1: DStream[String] = lines.map(
      data =>{
        //Code:Executor端
        data
      }
    )

    //由于SparkStreaming采集器是长期执行的任务，所以不能直接关闭
    //如果main方法执行完毕，应用程序也会自动结束
//    streamingContext.stop()
//    //1.启动采集器
    streamingContext.start()
//    //2.等待采集器的关闭
    streamingContext.awaitTermination()


  }

}

