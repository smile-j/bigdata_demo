package com.demo.bigdata.spark_core.rdd.framework.util

import org.apache.spark.SparkContext

object EnvUtil {

  private val scLocal = new ThreadLocal[SparkContext]

  def put(sc:SparkContext): Unit ={
    scLocal.set(sc)
  }

  def get()={
    scLocal.get()
  }

  def clear ={
    scLocal.remove()
  }

}
