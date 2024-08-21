package com.demo.bigdata.spark_core.rdd.operator.transform

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD


object Spark15_RDD_Operator_Transform {


  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
      .setMaster("local")
      .setAppName("WorldCount")
    import org.apache.spark.SparkContext
    val sc = new SparkContext(sparkConf)

    val rdd = sc.makeRDD(List(
      ("a", 1), ("a", 2), ("b", 3)
      ,("b", 4),("b", 5),("a", 6))
      ,2)


    /**
      * reduceByKey：
      *     combineByKeyWithClassTag[V]((v: V) => v, func, func, partitioner)
      *
      *
      *
      */

    rdd.reduceByKey(_ + _)
    rdd.aggregateByKey(0)(_ + _,_ + _)
    rdd.foldByKey(0)(_ + _)
    rdd.combineByKey(v=>v,(x:Int,y:Int)=>x+y,(x:Int,y:Int)=>x+y)
      .collect().foreach(println)

    //关闭连接
    sc.stop()
  }

}
