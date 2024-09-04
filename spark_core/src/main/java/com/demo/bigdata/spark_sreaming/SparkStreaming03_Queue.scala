package com.demo.bigdata.spark_sreaming

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable

object SparkStreaming03_Queue {


  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("spark_streaming")
    val streamingContext: StreamingContext = new StreamingContext(sparkConf,Seconds(3))

    val rddQueue: mutable.Queue[RDD[Int]] = new mutable.Queue[RDD[Int]]()

    val inputStream = streamingContext.queueStream(rddQueue,oneAtATime = false)

    val mappedStream = inputStream.map((_,1))
    val reduceStream = mappedStream.reduceByKey((_ + _))
    reduceStream.print()


//    streamingContext.stop()
//    //1.启动采集器
    streamingContext.start()

    for (i<- 1 to 5 ){
      rddQueue += streamingContext.sparkContext.makeRDD(1 to 300,10)
      Thread.sleep(2000)
    }

//    //2.等待采集器的关闭
    streamingContext.awaitTermination()


  }

}

