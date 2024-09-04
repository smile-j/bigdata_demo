package com.demo.bigdata.spark_sreaming

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.{InputDStream, ReceiverInputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.util.Random

object SparkStreaming06_State {


  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("spark_streaming")
    val ssc: StreamingContext = new StreamingContext(sparkConf,Seconds(3))

    ssc.checkpoint("cp")

    //获取数据
    //无状态数据操作，只对当前采取周期内的数据进行处理
    //在某些场合下，需要保留数据统计结果（状态），实现数据汇总
    //使用有状态操作时，需要设定检查点路径
    val lines: ReceiverInputDStream[String] = ssc.socketTextStream("localhost",9999)

    val words = lines.flatMap(_.split(" "))

    val wordToOne = words.map((_,1))
//    val wordToCount = wordToOne.reduceByKey(_+_)

    //updateStateByKey:根据key对数据的状态进行更新
    //参数
    // 参数1:表示相同的key的value数据
    // 参数2:表示缓存区相同key的value数据
    val state = wordToOne.updateStateByKey(
      (seq: Seq[Int], buff: Option[Int]) => {
        val newCount = buff.getOrElse(0) + seq.sum
        Option(newCount)
      }
    )
    state.print()


//    streamingContext.stop()
//    //1.启动采集器
    ssc.start()

//    //2.等待采集器的关闭
    ssc.awaitTermination()


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

