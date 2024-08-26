package com.demo.bigdata.spark_core.rdd.operator.action

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD

object Spark05_RDD_Operator_Save {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
      .setMaster("local")
      .setAppName("WorldCount")
    import org.apache.spark.SparkContext
    val sc = new SparkContext(sparkConf)

    /**
      * 行动算子
      */

    val rdd:RDD[(String,Int)] = sc.makeRDD(List(("a",1),("a",2),("a",3),("a",4)))

    rdd.saveAsTextFile("output1")
    rdd.saveAsObjectFile("output2")
//    必须key value
    rdd.saveAsSequenceFile("output3")



    //关闭连接
    sc.stop();
  }

}
