package com.demo.bigdata.spark_core.rdd.operator.transform

import org.apache.spark.SparkConf

object Spark06_RDD_Operator_Transform {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
      .setMaster("local")
      .setAppName("WorldCount")
    import org.apache.spark.SparkContext
    val sc = new SparkContext(sparkConf)

    //todo 算子（方法、操作） groupBy
    //会将数据打散，重新组合，这个操作我们称之为shuffle

    val rdd = sc.makeRDD(List(1,2,3,4),2)

    val value = rdd.groupBy(e=>e>3)
    value.collect().foreach(println)
    //关闭连接
    sc.stop();
  }

}
