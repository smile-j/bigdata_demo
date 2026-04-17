package com.demo.bigdata.spark_sreaming

import java.sql.ResultSet
import java.text.SimpleDateFormat
import java.util.Date

import com.demo.bigdata.util.JDBCUtil
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable.ListBuffer
import scala.util.Random

object SparkStreaming10_ReqBlackList2 {


  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("spark_streaming")
    val ssc: StreamingContext = new StreamingContext(sparkConf,Seconds(3))

    //3.定义Kafka参数
    val kafkaPara: Map[String, Object] = Map[String, Object](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG ->
        "linux1:9092,linux2:9092,linux3:9092",
      ConsumerConfig.GROUP_ID_CONFIG -> "test_topic",
      "key.deserializer" ->
        "org.apache.kafka.common.serialization.StringDeserializer",
      "value.deserializer" ->
        "org.apache.kafka.common.serialization.StringDeserializer"
    )

    val kafkaDataDS: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](Set("test_topic"), kafkaPara))

    val adClickData: DStream[AdClickData] = kafkaDataDS.map(
      kafkaData => {
        val datas = kafkaData.value().split(" ")
        AdClickData(datas(0), datas(1), datas(2), datas(3), datas(4))

      }
    )

    adClickData

    val ds: DStream[((String, String, String), Int)] = adClickData.transform(

      rdd => {
        //周期性获取黑名单数据
        var blackList = ListBuffer[String]()

        val connection = JDBCUtil.getConnection
        val preparedStatement = connection.prepareStatement("select user_id from black_list ")
        val res: ResultSet = preparedStatement.executeQuery()
        while (res.next()) {
          blackList.append(res.getString(1))
        }
        res.close()
        preparedStatement.close()
        connection.close()

        val filterRdd = rdd.filter(
          data => {
            //判断点击用户是否在黑名单中
            !blackList.contains(data.user)
          }
        )
        //如果用户不在黑名单中，那么进行统计数量（每个采集周期）
        val tupleRdd: RDD[((String, String, String), Int)] = filterRdd.map(
          data => {
            val sdf = new SimpleDateFormat("yyyy-MM-dd")
            val day = sdf.format(new Date(data.ts.toLong))
            val user = data.user
            val ad = data.ad

            ((day, user, ad), 1)
          }
        )
        tupleRdd.reduceByKey(_ + _)
      }
    )

    //判断更新后的点击数是否超过阈值，如果超过，那么将用户拉入黑名单

    ds.foreachRDD(rdd=>{

      //RDD提供了一个算子可以有效提升效率：foreachPartition
      //可以一个分区创建一个连接对象，这样可以大幅度减少连接对象的数量，提升效率
//      rdd.foreachPartition(iter=>{
//        val connection = JDBCUtil.getConnection
//        iter.foreach{
//          case((day, user, ad), count)=>{
////            connection....
//        }}
//      })

      rdd.foreach{
        case((day, user, ad), count)=>{
          //如果统计数量超过点击阈值(30)，那么将用户拉入到黑名单
          if(count>=30){
            val connection = JDBCUtil.getConnection
            val preparedStatement = connection.prepareStatement(
              """
                |insert into black_list(user_id) values(?)
                |ON DUPLICATE KEY
                |UPDATE user_id = ?
              """.stripMargin)
            preparedStatement.setString(1,user)
            preparedStatement.setString(2,user)
            preparedStatement.executeUpdate()
            connection.close()
          }else{
            //如果没有超过阈值，那么需要将当天的广告点击进行聚合
            val connection = JDBCUtil.getConnection
            val preparedStatement = connection.prepareStatement(
              """
                |select * from user_ad_count
                |where dt = ? user_id = ? and adid = ?
              """.stripMargin)
            preparedStatement.setString(1,day)
            preparedStatement.setString(2,user)
            preparedStatement.setString(3,ad)

            val resultSet = preparedStatement.executeQuery()
            if(resultSet.next()){
              val ppsatm = connection.prepareStatement(
                """
                  | update user_ad_count
                  | set count = count + ?
                  | where dt = ? user_id = ? and adid = ?
                """.stripMargin)
              ppsatm.setInt(1,count)
              ppsatm.setString(2,day)
              ppsatm.setString(3,user)
              ppsatm.setString(4,ad)
              ppsatm.executeUpdate()
              ppsatm.close()

              val oldCount = resultSet.getInt("count")
              if(oldCount+count >30 ){
                val connection = JDBCUtil.getConnection
                val preparedStatement = connection.prepareStatement(
                  """
                    |insert into black_list(user_id) values(?)
                    |ON DUPLICATE KEY
                    |UPDATE user_id = ?
                  """.stripMargin)
                preparedStatement.setString(1,user)
                preparedStatement.setString(2,user)
                preparedStatement.executeUpdate()
                connection.close()
              }

            }else{
              val ppst = connection.prepareStatement(
                """
insert into user_ad_count(dt,user_id,adid,dt)
values(?,?,?,?)
                """.stripMargin)
              ppst.setString(1,day)
              ppst.setString(2,user)
              ppst.setString(3,ad)
              ppst.setInt(4,1)
              ppst.execute()
              ppst.close()
            }
            resultSet.close()
            preparedStatement.close()
            connection.close()

          }
        }
      }
    })





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

  //广告实体
  case class AdClickData(ts:String,area:String,city:String,user:String,ad:String){

  }
}

