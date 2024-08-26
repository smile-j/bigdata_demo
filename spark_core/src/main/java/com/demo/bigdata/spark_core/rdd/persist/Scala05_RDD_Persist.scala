package com.demo.bigdata.spark_core.rdd.persist

import org.apache.spark.{SparkConf, SparkContext}

object Scala05_RDD_Persist {

  def main(args: Array[String]): Unit = {

    /**
      * 持久化
      *
      * cache:将数据临时存储在内存中进行数据重用
      *       会在血缘关系中添加新的依赖.一旦出现问题,可以重头读取数据
      * persist:将数据临时存储在磁盘中进行数据重用
      *         涉及到磁盘IO,性能较低,但是数据安全不高
      *         如果作业执行完毕,临时保存的数据就会丢失
      * checkpoint:将数据长久保存到磁盘中进行数据重用
      *             为了保证数据安全,所以一般情况下,会独立执行作业
      *             为了能够提高效率,一般情况下,需要和Cache联合使用
      *             执行过程中,会切断血缘关系.重新建立新的血缘关系
      *             checkpoint等同于改变数据源
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
//    mapRdd.cache()
    mapRdd.checkpoint()
    println(mapRdd.toDebugString)
    val reduceRdd = mapRdd.reduceByKey( _ + _ )
    reduceRdd.collect().foreach(println)
    println("*"*20)
    println(mapRdd.toDebugString)

    //关闭连接
    sc.stop();

  }

}
