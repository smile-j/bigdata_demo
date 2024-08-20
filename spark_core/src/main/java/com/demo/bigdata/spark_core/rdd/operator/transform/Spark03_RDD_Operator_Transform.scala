package com.demo.bigdata.spark_core.rdd.operator.transform

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD

object Spark03_RDD_Operator_Transform {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
      .setMaster("local")
      .setAppName("WorldCount")
    import org.apache.spark.SparkContext
    val sc = new SparkContext(sparkConf)

    //todo 算子（方法、操作） mapPartitionsWithIndex
    //


    val rdd = sc.makeRDD(List(1,2,3,4))


//    val mapRdd:RDD[Int] = rdd.mapPartitionsWithIndex((index,iterable) => {
//      println(">>>>>>>>"+index)
//      if(index==1){
//        iterable
//      }else{
//        Nil.iterator
//      }
//    })
    val mapRdd = rdd.mapPartitionsWithIndex((index, iterable) => {
      iterable.map((num) => {
        (index, num)
      })
    })
    mapRdd.collect().foreach(println)



    //关闭连接
    sc.stop();
  }

}
