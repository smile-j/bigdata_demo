package com.demo.bigdata.spark_core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


object Spark13_RDD_Operator_Transform_aggregateByKey {


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
      (x,y)=>math.max(x,y)
      ,(x,y)=>x+y).collect().foreach(println)
    println("=="*10)
    test1(sc)

    //关闭连接
    sc.stop()
  }

  def test1(sc:SparkContext): Unit ={
    /**
      * 计算相同key的数据平均值
      */
    val rdd = sc.makeRDD(List(
      ("a", 1), ("a", 2), ("b", 3)
      ,("b", 4),("b", 5),("a", 6))
      ,2)

    val newRdd:RDD[(String,(Int,Int))] = rdd.aggregateByKey((0: Int, 0: Int))(
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

  }

}
