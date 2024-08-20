package com.demo.bigdata.spark_core.rdd.operator.transform

import org.apache.spark.SparkConf

object Spark04_RDD_Operator_Transform {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
      .setMaster("local")
      .setAppName("WorldCount")
    import org.apache.spark.SparkContext
    val sc = new SparkContext(sparkConf)

    //todo 算子（方法、操作） flatMap
    //


//    val rdd = sc.makeRDD(List(List(1,2),List(3,4)))
//    val value = rdd.flatMap(list=>list)
//    val value = sc.makeRDD(List("hell0 spark","hello scala")).flatMap(_.split(" "))
    val value = sc.makeRDD(List(List(1,2),5,List(3,4)))
  .flatMap(
    iter =>{
      iter match{
        case list: List[_] => list
        case data =>List(data)
      }
  })


    value.collect().foreach(println)



    //关闭连接
    sc.stop();
  }

}
