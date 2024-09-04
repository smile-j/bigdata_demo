package com.demo.bigdata.spark_sreaming

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.{InputDStream, ReceiverInputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.util.Random

object SparkStreaming05_Kafka {


  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("spark_streaming")
    val ssc: StreamingContext = new StreamingContext(sparkConf,Seconds(3))

    //3.定义Kafka参数
    val kafkaPara: Map[String, Object] = Map[String, Object](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG ->
        "linux1:9092,linux2:9092,linux3:9092",
      ConsumerConfig.GROUP_ID_CONFIG -> "atguigu",
      "key.deserializer" ->
        "org.apache.kafka.common.serialization.StringDeserializer",
      "value.deserializer" ->
        "org.apache.kafka.common.serialization.StringDeserializer"
    )

    val kafkaDataDS: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](Set("atguigu"), kafkaPara))

    kafkaDataDS.map(_.value()).print()

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

