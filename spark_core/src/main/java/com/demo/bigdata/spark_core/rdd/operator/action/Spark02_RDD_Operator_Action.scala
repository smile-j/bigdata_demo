package com.demo.bigdata.spark_core.rdd.operator.action

import org.apache.spark.SparkConf

object Spark02_RDD_Operator_Action {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
      .setMaster("local")
      .setAppName("WorldCount")
    import org.apache.spark.SparkContext
    val sc = new SparkContext(sparkConf)

    /**
      * 行动算子
      */

    val rdd = sc.makeRDD(List(1,2,3,4))

    val sum = rdd.reduce(_ + _)
    println(sum)

    //collect 将不同分区的数据按照分区顺序采集到内存中
    val array:Array[Int] = rdd.collect()
    println(array.mkString(","))


    val count = rdd.count()
    println(count)

    val firtItem:Int = rdd.first()

    val newArray:Array[Int] = rdd.take(2)

    //排序后取前n个
    val rdd2 = sc.makeRDD(List(4,2,1,3))
    rdd2.takeOrdered(3)
    rdd2.takeOrdered(3)(Ordering.Int.reverse)

    //关闭连接
    sc.stop();
  }

}
