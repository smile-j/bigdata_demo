package com.demo.bigdata.spark_core.rdd.operator.transform

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD

object Spark05_RDD_Operator_Transform {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
      .setMaster("local")
      .setAppName("WorldCount")
    import org.apache.spark.SparkContext
    val sc = new SparkContext(sparkConf)

    //todo 算子（方法、操作） glom
    //将同一个分区的数据直接转换为相同类型的内存数组进行处理，分区不变

//    val rdd:RDD[Int] = sc.makeRDD(List(1,2,3,4),2)
//    val glomRdd:RDD[Array[Int]] = rdd.glom()
//    glomRdd.collect().foreach(data=>println(data.mkString(",")))

    //test
    //分组后求最大值 再求和
    val sum = sc.makeRDD(List(1, 2, 3, 4), 2)
      .glom()
      .map(array => array.max).collect()
      .sum
    println(sum)

    //关闭连接
    sc.stop();
  }

}
