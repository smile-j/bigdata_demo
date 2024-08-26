package com.demo.bigdata.spark_core.rdd.acc

import org.apache.spark.{SparkConf, SparkContext}

object Spark01_Acc {

  def main(args: Array[String]): Unit = {

    /**
      * 累加器
      *
      */

    var sparkConf = new SparkConf().setMaster("local").setAppName("wordCount")
    val sc = new SparkContext(sparkConf)

    val rdd = sc.makeRDD(List(1,2,3,4))

//    val sum = rdd.reduce(_ + _)

    var sum = 0
    rdd.foreach(item=>{
      sum+=item
      println("**********"+sum)
    })
    println(sum)

    sc.stop()

  }

}
