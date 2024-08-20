package com.demo.bigdata.spark_core.rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

object Spark01_RDD_Operator_Transform {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
      .setMaster("local")
      .setAppName("WorldCount")
    import org.apache.spark.SparkContext
    val sc = new SparkContext(sparkConf)

    //todo 算子（方法、操作） map

    val rdd = sc.makeRDD(List(1,2,3,4))

    def mapFun(num:Int):Int={
      num*2
    }
//    val mapRdd = rdd.map(mapFun)
    val mapRdd = rdd.map(_*2)
    mapRdd.collect().foreach(println)

    //关闭连接
    sc.stop();
  }

}
