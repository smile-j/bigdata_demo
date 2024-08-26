package com.demo.bigdata.spark_core.rdd.io

import org.apache.spark.rdd.RDD
import org.apache.spark.{Partitioner, SparkConf, SparkContext}

object Spark01_RDD_save {

  /**
    * save
    */

  def main(args: Array[String]): Unit = {
    var sparkConf = new SparkConf().setMaster("local").setAppName("wordCount")
    val sc = new SparkContext(sparkConf)

    val rdd:RDD[(String,Int)] = sc.makeRDD(List(
      ("a", 1),
      ("b", 2),
      ("c", 3),
      ("d", 4)
    ), 3)

    rdd.saveAsTextFile("output1")
    rdd.saveAsObjectFile("output2")
    rdd.saveAsSequenceFile("output3")


    sc.stop()
  }



}
