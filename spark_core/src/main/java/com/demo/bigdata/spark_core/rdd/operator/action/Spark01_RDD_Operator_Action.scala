package com.demo.bigdata.spark_core.rdd.operator.action

import org.apache.spark.SparkConf

object Spark01_RDD_Operator_Action {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
      .setMaster("local")
      .setAppName("WorldCount")
    import org.apache.spark.SparkContext
    val sc = new SparkContext(sparkConf)

    /**
      * 行动算子：触发作业（job）执行
      * 底层代码调用的是环境对象的runJob方法
      * 底层代码中创建ActiveJob,并提交执行
      */

    val rdd = sc.makeRDD(List(1,2,3,4))

    rdd.collect()


    //关闭连接
    sc.stop();
  }

}
