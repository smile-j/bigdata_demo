package com.demo.bigdata.spark_core.rdd.framework.common

import com.demo.bigdata.spark_core.rdd.framework.controller.WordCountController
import com.demo.bigdata.spark_core.rdd.framework.util.EnvUtil
import org.apache.spark.{SparkConf, SparkContext}

trait TApplication {

  def start(master:String = "local[*]",appName:String = "Application")(op : => Unit): Unit ={
    val sparkConf = new SparkConf().setMaster(master).setAppName(appName)
    val sc = new SparkContext(sparkConf)

    EnvUtil.put(sc)

    try {
      op
    }catch {
      case ex => println(ex.getMessage)
    }

    //关闭连接
    sc.stop()
  }



}
