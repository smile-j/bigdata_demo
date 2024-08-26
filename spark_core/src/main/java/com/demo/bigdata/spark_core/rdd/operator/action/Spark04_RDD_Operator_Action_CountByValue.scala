package com.demo.bigdata.spark_core.rdd.operator.action

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD

import scala.collection.Map

object Spark04_RDD_Operator_Action_CountByValue {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
      .setMaster("local")
      .setAppName("WorldCount")
    import org.apache.spark.SparkContext
    val sc = new SparkContext(sparkConf)

    /**
      * 行动算子
      * countByValue:统计元素出现的次数
      */

    val rdd1 = sc.makeRDD(List(1,2,3,4,3))
    val value1:Map[Int,Long] = rdd1.countByValue()
    println(value1)
    //统计key出现的次数
    val rdd = sc.makeRDD(List(
  ("a",1),("a",2),("a",3)
))
    val value = rdd.countByKey()
    println(value)

    //关闭连接
    sc.stop();
  }

}
