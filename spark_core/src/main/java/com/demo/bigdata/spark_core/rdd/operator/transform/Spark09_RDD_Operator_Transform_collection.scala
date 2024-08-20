package com.demo.bigdata.spark_core.rdd.operator.transform

import org.apache.spark.SparkConf

object Spark09_RDD_Operator_Transform_collection {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
      .setMaster("local")
      .setAppName("WorldCount")
    import org.apache.spark.SparkContext
    val sc = new SparkContext(sparkConf)

    //todo 算子（方法、操作） 集合
    //交集 并集 差集  拉链


    val rdd1 = sc.makeRDD(List(1,2,3,4))
    val rdd2 = sc.makeRDD(List(6,5,4,3))

    // 交集
    val rdd3 = rdd1.intersection(rdd2)
    println(rdd3.collect().mkString(","))
    // 并集
    val rdd4 = rdd1.union(rdd2)
    println(rdd4.collect().mkString(","))
    // 差集
    val rdd5 = rdd1.subtract(rdd2)
    println(rdd5.collect().mkString(","))

    // 拉链
    //数据源的类型可以不一致
    //俩个数据源的必须长度一致；分区数量必须一致
    val rdd6 = rdd1.zip(rdd2)
    println(rdd6.collect().mkString(","))

    //关闭连接
    sc.stop()
  }

}
