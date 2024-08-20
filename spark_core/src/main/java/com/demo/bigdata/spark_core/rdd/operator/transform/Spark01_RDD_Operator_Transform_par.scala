package com.demo.bigdata.spark_core.rdd.operator.transform

import org.apache.spark.SparkConf

object Spark01_RDD_Operator_Transform_par {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
      .setMaster("local")
      .setAppName("WorldCount")
    import org.apache.spark.SparkContext
    val sc = new SparkContext(sparkConf)

    //todo 算子（方法、操作） map

    val rdd = sc.makeRDD(List(1,2,3,4),2)

    val mapRdd = rdd.map(num=>{
      println("11:"+num)
      num*2
    })
    val mapRdd2 = rdd.map(num=>{
      println("22:"+num)
      num
    })
    mapRdd.collect()
    mapRdd2.collect()
//    mapRdd.collect().foreach(println)

    //关闭连接
    sc.stop();
  }

}
