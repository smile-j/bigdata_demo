package com.demo.bigdata.spark_core.rdd.operator.transform

import org.apache.spark.SparkConf


object Spark17_RDD_Operator_Transform_cogroup {


  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
      .setMaster("local")
      .setAppName("WorldCount")
    import org.apache.spark.SparkContext
    val sc = new SparkContext(sparkConf)

    val rdd1 = sc.makeRDD(List(
      ("a", 1), ("b", 2)
    ))
    val rdd2 = sc.makeRDD(List(
      ("a", 3),("b", 4), ("c", 5), ("c", 6)
    ))

    //concat + group  (分组+连接)
    rdd1.cogroup(rdd2).collect().foreach(println)

    //关闭连接
    sc.stop()
  }

}
