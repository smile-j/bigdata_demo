package com.demo.bigdata.spark_sreaming

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable.ListBuffer
import scala.util.Random

object SparkStreaming10_MockData {


  def main(args: Array[String]): Unit = {

      //模拟生成数据

      //格式 ：timestamp area city userid adid
      //       某个时间点 某个地区 某个城市 某个用户 某个广告

      //Application => Kafka => SparkStreaming => Analysis

      // 创建配置对象
//      val prop = new Properties()
//      // 添加配置
//      prop.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop:9092")
//      prop.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
//        "org.apache.kafka.common.serialization.StringSerializer")
//      prop.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
//        "org.apache.kafka.common.serialization.StringSerializer")
//      val producer = new KafkaProducer[String,String](prop)

      while (true){
        mockData().foreach(
          data =>{
            //向kafka生产数据
//            val record = new ProducerRecord[String,String]("test_topic",data)
//            producer.send(record)
            println(data)
          }
        )
        Thread.sleep(2000)
      }

    }

    def mockData()={
      val list = ListBuffer[String]()
      val areaList = ListBuffer[String]("华东","华北","华南")
      val cityList = ListBuffer[String]("北京","上海","广州","深圳")
      for (i<- 1 to 30){

        val area = areaList(new Random().nextInt(3))
        val city = cityList(new Random().nextInt(4))
        val userId = new Random().nextInt(6)+1
        val adId = new Random().nextInt(6)+1
        val currentTime = System.currentTimeMillis()


        list.append(s"${currentTime} ${area} ${city} ${userId} ${adId}")
      }
      list
    }



}

