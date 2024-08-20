package com.demo.bigdata.spark_core.rdd.operator.transform

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD

object Spark02_RDD_Operator_Transform {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
      .setMaster("local")
      .setAppName("WorldCount")
    import org.apache.spark.SparkContext
    val sc = new SparkContext(sparkConf)

    //todo 算子（方法、操作） mapPartitions
    //可以将分区为单位进行数据操作
    //但是会将整个分区的数据加载到内存中进行应用
    //如果处理完的数据是不会被释放掉，存在对象的应用
    //在内存较小，数据量较大的场合，容易出现内存溢出

    val rdd = sc.makeRDD(List(1,2,3,4),2)

//    val mapRdd:RDD[Int] = rdd.mapPartitions((iterable) => {
//      println(">>>>>>>>>>>>>>")
//      iterable.map(_ * 2)
//    })
    val mapRdd:RDD[Int] = rdd.mapPartitions((iterable) => {
      List(iterable.max).iterator
    })
    mapRdd.collect().foreach(println)



    //关闭连接
    sc.stop();
  }

}
