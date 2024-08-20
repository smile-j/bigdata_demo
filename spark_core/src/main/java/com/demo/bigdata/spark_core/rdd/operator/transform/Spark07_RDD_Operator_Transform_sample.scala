package com.demo.bigdata.spark_core.rdd.operator.transform

import org.apache.spark.SparkConf

object Spark07_RDD_Operator_Transform_sample{

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
      .setMaster("local")
      .setAppName("WorldCount")
    import org.apache.spark.SparkContext
    val sc = new SparkContext(sparkConf)

    //todo 算子（方法、操作） sample
    //参数说明：3个参数
    //第一个 抽取后是否放回
    //第二个 每条数据可能抽取的概率
    //第三个 抽取算法时 随机算法的种子,如果不传 那么使用的是当前的系统时间

    val rdd = sc.makeRDD(List(1,2,3,4,1,2,3,4))

    println(rdd.distinct().collect().mkString(","))


    //关闭连接
    sc.stop();
  }

}
