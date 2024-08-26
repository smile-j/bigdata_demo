package com.demo.bigdata.spark_core.rdd.persist

import org.apache.spark.{SparkConf, SparkContext}

object Scala03_RDD_Persist {

  def main(args: Array[String]): Unit = {

    /**
      * 持久化
      * 
      */

    val sparkConf = new SparkConf()
      .setMaster("local")
      .setAppName("WorldCount");
    val sc = new SparkContext(sparkConf);

    sc.setCheckpointDir("cp")

    val rdd = sc.makeRDD(List("hello scala","hello spark"))

    val flatMap = rdd.flatMap(_.split(" "))

    val mapRdd = flatMap.map((_,1))

    //checkpoint需要落盘,需要指定检查点保存路径
    //检查点路径保存的文件,当作业执行完毕后,不会被删除
    //一般保存路径都是分布式存储系统:HDFS
   mapRdd.checkpoint()

    val reduceRdd = mapRdd.reduceByKey( _ + _ )
    reduceRdd.collect().foreach(println)
    println("*"*20)

    val result2 = mapRdd.groupByKey()
    result2.collect().foreach(println)

    //关闭连接
    sc.stop();

  }

}
