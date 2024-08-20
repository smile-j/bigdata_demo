package com.demo.bigdata.spark_core.rdd.operator.transform

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD

object Spark12_RDD_Operator_Transform_GroupByKey {

  /**
    * spark，shuffle 操作必须落盘处理，不能在内存中等待数据，会导致内存溢出
    * shuffle操作的性能非常低
    *
    */

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
      .setMaster("local")
      .setAppName("WorldCount")
    import org.apache.spark.SparkContext
    val sc = new SparkContext(sparkConf)

    //todo 算子（方法、操作） key-value类型
    //groupByKey 会被打乱重新组合，有shuffle操作
    //将数据源中的数据，相同key的数据分在一个组中，形成一个对偶元组
    //元组中第一个元素就是key
    //元组中的第二个元素就是相同key的value集合

    val rdd = sc.makeRDD(List(("a", 1), ("b", 2), ("a", 3), ("b", 4)))
    val newRdd:RDD[(String, Iterable[Int])] = rdd.groupByKey()
    newRdd.collect().foreach(println)
    val newRdd2:RDD[(String, Int)] = rdd.reduceByKey((v1,v2)=>v1+v2)
    newRdd2.collect().foreach(println)
    val newRDD3:RDD[(String,Iterable[(String,Int)])] = rdd.groupBy((entry) => entry._1)
    newRDD3.collect().foreach(println)
    //关闭连接
    sc.stop()
  }

}
