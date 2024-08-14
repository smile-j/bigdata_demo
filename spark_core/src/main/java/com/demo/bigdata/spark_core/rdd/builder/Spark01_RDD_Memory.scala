package com.demo.bigdata.spark_core.rdd.builder

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark01_RDD_Memory {

  /**
    * 从内存创建RDD
    */

  def main(args: Array[String]): Unit = {

    //准备环境
    val conf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(conf)

    //创建RDD
    var seq = Seq[Int](1,2,3,4)
    //parallelize 并行
//    val rdd:RDD[Int] = sc.parallelize(seq)
    //makeRDD 底层就是调用了rdd对象的parallelize方法
      val rdd:RDD[Int] = sc.makeRDD(seq)
    rdd.collect().foreach(e=>println("threadMain:"+Thread.currentThread().getName+",value:"+e))

    //关闭环境
    sc.stop()

  }

}
