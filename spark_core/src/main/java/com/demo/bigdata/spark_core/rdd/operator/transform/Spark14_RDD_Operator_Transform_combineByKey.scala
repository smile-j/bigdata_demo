package com.demo.bigdata.spark_core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


object Spark14_RDD_Operator_Transform_combineByKey {


  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
      .setMaster("local")
      .setAppName("WorldCount")
    import org.apache.spark.SparkContext
    val sc = new SparkContext(sparkConf)

    //todo 算子（方法、操作） key-value类型
    //combineByKey  有三个参数
    //第一个参数：将相同key的第一个数据进行结构的转换
    //第二个参数：分区内的计算规则
    //第三个参数：分区间的计算规则

    val rdd = sc.makeRDD(List(
      ("a", 1), ("a", 2), ("b", 3)
      ,("b", 4),("b", 5),("a", 6))
      ,2)

    val newRdd:RDD[(String,(Int,Int))] = rdd.combineByKey(
      v=>(v,1),
      (t, v) => {
        (t._1 + v, t._2 + 1)
      },
      (t1, t2) => {
        (t1._1 + t2._1, t1._2 + t2._2)
      }
    )
    newRdd.mapValues({
      case (num,cnt)=>num/cnt
    })
      .collect().foreach(println)
    //关闭连接
    sc.stop()
  }

}
