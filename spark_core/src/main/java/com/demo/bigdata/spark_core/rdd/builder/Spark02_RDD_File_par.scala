package com.demo.bigdata.spark_core.rdd.builder

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark02_RDD_File_par {

  /**
    * 从文件创建RDD
    */

  def main(args: Array[String]): Unit = {

    //准备环境
    val conf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(conf)

    //创建RDD
//    textFile将可以将文件作为数据源，默认也可以设定分区
    //  spark读取文件,底层其实使用的就是hadoop的读取方式
    //    数据读取时以偏移量为单位，偏移量不会被重复读取
    val rdd:RDD[String] = sc.textFile("datas/*1.txt",3)
    rdd.saveAsTextFile("outPath")
    rdd.collect().foreach(println)
    //关闭环境
    sc.stop()

  }

}
