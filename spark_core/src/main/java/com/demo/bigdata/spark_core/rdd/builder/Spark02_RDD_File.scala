package com.demo.bigdata.spark_core.rdd.builder

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark02_RDD_File {

  /**
    * 从文件创建RDD
    */

  def main(args: Array[String]): Unit = {

    //准备环境
    val conf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(conf)

    //创建RDD
    //path路径默认是以当前环境的根路径为基准，可以写绝对路径，也可以写相对路径
    //path可以是文件的具体路径，也可以是目录名称
//    val rdd:RDD[String] = sc.textFile("D:\\project\\github\\bigdata_demo\\bigdata_demo\\datas\\1.txt")
//    val rdd:RDD[String] = sc.textFile("datas/1.txt")
//    val rdd:RDD[String] = sc.textFile("datas")
    //path还可以使用通配符 *
    val rdd:RDD[String] = sc.textFile("datas/*1.txt")
    //path还可以是分布式存储系统路径:HDFS
//    val rdd:RDD[String] = sc.textFile("hdfs://hadoop102:/input/sanguo.world")
    rdd.collect().foreach(println)
    //关闭环境
    sc.stop()

  }

}
