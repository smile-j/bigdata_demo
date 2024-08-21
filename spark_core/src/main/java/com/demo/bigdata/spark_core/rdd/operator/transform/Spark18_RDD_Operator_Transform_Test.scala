package com.demo.bigdata.spark_core.rdd.operator.transform

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD


object Spark18_RDD_Operator_Transform_Test {


  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
      .setMaster("local")
      .setAppName("WorldCount")
    import org.apache.spark.SparkContext
    val sc = new SparkContext(sparkConf)

    /**
      * agent.log: 时间戳，省份，城市，用户 广告 中间空格隔开
      * 统计每个一个省份每个广告被点击数量的排行的top3
      */

    //1.获取数据
    //2.数据转换：（（省份，广告），1）
    //3.分组聚合:((省份，广告)，sum)
    //4.聚合的结果的转换：（省份，（广告，sum））
    //5.转换后的数据根据省份进行分组：（省份，【（广告a,sum_a）,（广告b,sum_b）】）
    //6.分组后的数据 排序 取前三名
    val dataRdd = sc.textFile("datas/agent.log")
    val mappRdd:RDD[((String,String),Int)] = dataRdd.map(line => {
      val datas = line.split(" ")
      ((datas(1), datas(4)), 1)
    })
    val reduceRdd = mappRdd.reduceByKey(_ + _)
    val groupRdd = reduceRdd.map({
      case ((prv, ad), sum) => {
        (prv, (ad, sum))
      }
    }).groupByKey()

    val resuleRdd = groupRdd.mapValues(iter => {
      iter.toList.sortBy(_._2)(Ordering.Int.reverse).take(3)
    })
    resuleRdd.collect().foreach(println)
    //关闭连接
    sc.stop()
  }

}
