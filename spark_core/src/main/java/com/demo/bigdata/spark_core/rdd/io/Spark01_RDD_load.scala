package com.demo.bigdata.spark_core.rdd.io

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark01_RDD_load {

  /**
    * save
    */

  def main(args: Array[String]): Unit = {
    var sparkConf = new SparkConf().setMaster("local").setAppName("wordCount")
    val sc = new SparkContext(sparkConf)

    val rdd1 = sc.textFile("output1")
    println(rdd1.collect().mkString(","))

    val rdd2 = sc.objectFile[(String,Int)]("output2")
    println(rdd2.collect().mkString(","))

    val rdd3 = sc.sequenceFile[String,Int]("output3")
    println(rdd3.collect().mkString(","))


    sc.stop()
  }



}
