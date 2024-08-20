package com.demo.bigdata.spark_core.rdd.operator.transform

import org.apache.spark.SparkConf


object Spark13_RDD_Operator_Transform_aggregateByKey_2 {


  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
      .setMaster("local")
      .setAppName("WorldCount")
    import org.apache.spark.SparkContext
    val sc = new SparkContext(sparkConf)

    //todo 算子（方法、操作） key-value类型
    //aggregateByKey 存在函数柯里化，有俩个参数
    //第一个参数：需要传递一个值表示初始值
    //            主要用于碰见第一个key的时候，和value进行分区内计算
    //第二个参数：传递俩个函数
    //            第一个参数分区内计算规则
    //            第二个参数分区间计算规则

    val rdd = sc.makeRDD(List(("a", 1), ("a", 2), ("a", 3), ("a", 4)),2)
    //(a,[1,2]),(a,[3,4])
    //(a,2),(a,4)
    //(a,6)
    rdd.aggregateByKey(0)(
      (x,y)=>x+y
      ,(x,y)=>x+y).collect().foreach(println)
    rdd.aggregateByKey(0)(_+_,_+_)
      .collect().foreach(println)
    //如果聚合计算时，分区内和分区间计算规则相同，spark提供了简化的方法
    rdd.foldByKey(0)(_+_)
      .collect().foreach(println)

    //关闭连接
    sc.stop()
  }

}
