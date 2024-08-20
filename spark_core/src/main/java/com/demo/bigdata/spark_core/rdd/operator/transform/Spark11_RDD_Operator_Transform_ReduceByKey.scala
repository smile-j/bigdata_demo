package com.demo.bigdata.spark_core.rdd.operator.transform

import org.apache.spark.{HashPartitioner, SparkConf}

object Spark11_RDD_Operator_Transform_ReduceByKey {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
      .setMaster("local")
      .setAppName("WorldCount")
    import org.apache.spark.SparkContext
    val sc = new SparkContext(sparkConf)

    //todo 算子（方法、操作） key-value类型
//    reduceByKey ，分组聚合
    //    会被打乱重新组合，有shuffle操作


    val rdd = sc.makeRDD(List(("a", 1), ("b", 2), ("a", 3), ("b", 4)))
    //相同的key的数据进行value数据的聚合操作
    //
    rdd.reduceByKey((v1,v2)=>v1+v2).collect().foreach(println)

    //关闭连接
    sc.stop()
  }

}
