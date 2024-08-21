package com.demo.bigdata.spark_core.rdd.operator.action

import org.apache.spark.SparkConf

object Spark03_RDD_Operator_Action_Aggregate {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
      .setMaster("local")
      .setAppName("WorldCount")
    import org.apache.spark.SparkContext
    val sc = new SparkContext(sparkConf)



    val rdd = sc.makeRDD(List(1,2,3,4),2)

    //10 + 13 + 17
    //aggregateByKey:初始值只会参与分区内计算
    //aggregate：初始值会参与分区内计算,并且会参与分区间计算
//    val res = rdd.aggregate(10)(_ + _,_ + _)
//    println(res)

    //fold
    //分区内，分区间规则相同
    val res = rdd.fold(10)(_ + _)
    println(res)

    //关闭连接
    sc.stop();
  }

}
