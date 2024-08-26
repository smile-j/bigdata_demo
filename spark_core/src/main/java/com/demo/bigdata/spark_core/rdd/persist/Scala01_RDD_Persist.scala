package com.demo.bigdata.spark_core.rdd.persist

import org.apache.spark.{SparkConf, SparkContext}

object Scala01_RDD_Persist {

  def main(args: Array[String]): Unit = {

    /**
      * 持久化
      *
      */

    val sparkConf = new SparkConf()
      .setMaster("local")
      .setAppName("WorldCount");
    val sc = new SparkContext(sparkConf);

    val rdd = sc.makeRDD(List("hello scala","hello spark"))

    val flatMap = rdd.flatMap(_.split(" "))

    val mapRdd = flatMap.map((_,1))

    val reduceRdd = mapRdd.reduceByKey( _ + _ )
    reduceRdd.collect().foreach(println)
    println("*"*20)
    /**
      * Rdd中不存储数据
      * 如果一个RDD需要重复使用，那么需要从头再来执行获取数据
      * rdd对象可以重用，但是数据无法重用
      */
    val result2 = mapRdd.groupByKey()
    result2.collect().foreach(println)

    //关闭连接
    sc.stop();

  }

}
