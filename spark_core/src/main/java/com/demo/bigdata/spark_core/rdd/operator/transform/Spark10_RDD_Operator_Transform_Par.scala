package com.demo.bigdata.spark_core.rdd.operator.transform

import org.apache.spark.{HashPartitioner, RangePartitioner, SparkConf}

object Spark10_RDD_Operator_Transform_Par {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
      .setMaster("local")
      .setAppName("WorldCount")
    import org.apache.spark.SparkContext
    val sc = new SparkContext(sparkConf)

    //todo 算子（方法、操作） key-value类型


    val rdd = sc.makeRDD(List(("2", 1), ("3", 2), ("4", 3), ("5", 4)),2)
      //RDD => PairRDDFunctions
      //隐式转换
      //partitionBy 根据指定的分区规则对数据进行重分区
    val newRdd = rdd.partitionBy(new HashPartitioner(2))
//    val newRdd = rdd.partitionBy(new RangePartitioner(2))
    newRdd.saveAsTextFile("outPut")

    //关闭连接
    sc.stop()
  }

}
