package com.demo.bigdata.spark_core.rdd.acc

import org.apache.spark.{SparkConf, SparkContext}

object Spark02_Acc {

  def main(args: Array[String]): Unit = {


    var sparkConf = new SparkConf().setMaster("local").setAppName("wordCount")
    val sc = new SparkContext(sparkConf)

    val rdd = sc.makeRDD(List(1,2,3,4))

    /**
      * 累加器
      *
      */

      val sumAcc = sc.longAccumulator("sum")

    rdd.foreach(item=>{
      sumAcc.add(item)
    })
    println(sumAcc)
    println(sumAcc.value)

    sc.stop()

  }

}
