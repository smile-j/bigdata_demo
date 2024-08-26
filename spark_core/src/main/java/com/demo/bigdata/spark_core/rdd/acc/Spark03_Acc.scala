package com.demo.bigdata.spark_core.rdd.acc

import org.apache.spark.{SparkConf, SparkContext}

object Spark03_Acc {

  def main(args: Array[String]): Unit = {


    var sparkConf = new SparkConf().setMaster("local").setAppName("wordCount")
    val sc = new SparkContext(sparkConf)

    val rdd = sc.makeRDD(List(1,2,3,4))

    /**
      * 累加器
      *
      */

      val sumAcc = sc.longAccumulator("sum")

    val maprdd = rdd.map(item => {
      sumAcc.add(item)
      item
    })
    //少加:转换算子中调用累加器,如果没有行动算子的话,那么不会执行
    //多加:转换算子中调用累加器,多次执行
    //一般情况下,累加器会放置在行动算子进行操作
    maprdd.collect()
    maprdd.collect()
    println(sumAcc.value)

    sc.stop()

  }

}
