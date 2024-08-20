package com.demo.bigdata.spark_core.rdd.operator.transform

import org.apache.spark.SparkConf

object Spark06_RDD_Operator_Transform_filter {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
      .setMaster("local")
      .setAppName("WorldCount")
    import org.apache.spark.SparkContext
    val sc = new SparkContext(sparkConf)

    //todo 算子（方法、操作） filter

    val rdd = sc.makeRDD(List(1,2,3,4),2)

    val array = rdd.filter(elem=>elem%2==1).collect()
    array.foreach(println)

    //关闭连接
    sc.stop();
  }

}
