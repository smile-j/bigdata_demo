package com.demo.bigdata.spark_core.rdd.operator.transform

import org.apache.spark.SparkConf


object Spark16_RDD_Operator_Transform_leftJoin {


  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
      .setMaster("local")
      .setAppName("WorldCount")
    import org.apache.spark.SparkContext
    val sc = new SparkContext(sparkConf)

    val rdd1 = sc.makeRDD(List(
      ("a", 1), ("b", 2), ("d", 3)
    ))
    val rdd2 = sc.makeRDD(List(
      ("b", 4), ("c", 5),("a", 1),("a", 3)
    ))


    rdd1.leftOuterJoin(rdd2).collect().foreach(println)
    println("==="*10)
    rdd1.rightOuterJoin(rdd2).collect().foreach(println)

    //关闭连接
    sc.stop()
  }

}
