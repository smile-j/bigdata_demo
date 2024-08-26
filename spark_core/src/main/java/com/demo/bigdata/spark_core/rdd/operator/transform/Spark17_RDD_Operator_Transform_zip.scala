package com.demo.bigdata.spark_core.rdd.operator.transform

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD


object Spark17_RDD_Operator_Transform_zip {


  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
      .setMaster("local")
      .setAppName("WorldCount")
    import org.apache.spark.SparkContext
    val sc = new SparkContext(sparkConf)

    val rdd1 = sc.makeRDD(List(
      4,3,2,1
    ))
    val rdd2 = sc.makeRDD(List(
      "a","b","c","d"
    ))

    /**
      * zip
      *
      * 进行zip的RDD数据类型，可以不一致。
      * 进行zip的RDD，分区数量应一致。
      * 进行zip的RDD，分区中的数据数量应一致。
      */

    val res:RDD[(Int, String)] = rdd1.zip(rdd2)
    res.collect().foreach(println)

    //于scala的不同，scal不要求数据量一样
    val list1 = List(1,2,3,4)
    val list2 = List(1,2,3)
    println("**"*10)
    println(list1.zip(list2))
    println(list2.zip(list1))

    //关闭连接
    sc.stop()
  }

}
