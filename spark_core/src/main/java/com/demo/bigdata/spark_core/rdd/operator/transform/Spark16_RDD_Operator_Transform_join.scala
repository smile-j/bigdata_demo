package com.demo.bigdata.spark_core.rdd.operator.transform

import org.apache.spark.SparkConf


object Spark16_RDD_Operator_Transform_join {


  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
      .setMaster("local")
      .setAppName("WorldCount")
    import org.apache.spark.SparkContext
    val sc = new SparkContext(sparkConf)

    val rdd1 = sc.makeRDD(List(
      ("a", 1), ("b", 2), ("c", 3)
    ))
    val rdd2 = sc.makeRDD(List(
      ("b", 4), ("c", 5), ("d", 6),("a", 1),("a", 3)
    ))

    //join：俩个不同数据源的数据，相同的key的value连接在一起，形成元组
    //不同key的元素舍掉
    //俩个数据源中key有多个相同的，会依次匹配，出现笛卡尔积，数据量几何增长

    rdd1.join(rdd2).collect().foreach(println)

    //关闭连接
    sc.stop()
  }

}
