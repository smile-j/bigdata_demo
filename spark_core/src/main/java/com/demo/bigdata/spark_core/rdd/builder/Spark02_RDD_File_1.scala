package com.demo.bigdata.spark_core.rdd.builder

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark02_RDD_File_1 {

  /**
    * 从文件创建RDD
    */

  def main(args: Array[String]): Unit = {

    //准备环境
    val conf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(conf)

    //创建RDD

    //textFile:以行为单位来读取数据
    //wholeTextFiles:以文件为单位读取数据
    //    读取的结果表示为元组,第一个元素表示路径,第二个元素表示文件内容
   val rdd = sc.wholeTextFiles("datas")

    rdd.collect().foreach(println)
    //关闭环境
    sc.stop()

  }

}
