package com.demo.bigdata.spark_core.rdd.operator.transform

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD

object Spark09_RDD_Operator_Transform_sortBy {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
      .setMaster("local")
      .setAppName("WorldCount")
    import org.apache.spark.SparkContext
    val sc = new SparkContext(sparkConf)

    //todo 算子（方法、操作） sortBy
    //默认升序，第二个参数可以指定顺序
    //sortBy默认情况下，不会改变分区，但是中间存在shuffle操作
    //排序 有shuffle操作

//    val rdd = sc.makeRDD(List(5,2,6,4,1,3),2)
//    val newRdd:RDD[Int] = rdd.sortBy(num=>num)

    val newRdd = sc.makeRDD(List(("1", 1), ("11", 2), ("2", 2)))
      .sortBy(key => key)

    newRdd.saveAsTextFile("outPath")


    //关闭连接
    sc.stop()
  }

}
