package com.demo.bigdata.spark_core.rdd.operator.transform

import org.apache.spark.SparkConf

object Spark01_RDD_Operator_Transform_part {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
      .setMaster("local")
      .setAppName("WorldCount")
    import org.apache.spark.SparkContext
    val sc = new SparkContext(sparkConf)

    //todo 算子（方法、操作） map 转后，分区不变

    val rdd = sc.makeRDD(List(1,2,3,4),2)
    rdd.saveAsTextFile("outPut")
    rdd.map(_*2).mapPartitionsWithIndex((index,iter)=>{
      println(">>>>>>>>>>>>>"+index)
      iter
    })
    rdd.collect().foreach(println)

    //关闭连接
    sc.stop();
  }

}
