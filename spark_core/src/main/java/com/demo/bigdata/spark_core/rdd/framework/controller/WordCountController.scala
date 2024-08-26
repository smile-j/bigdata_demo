package com.demo.bigdata.spark_core.rdd.framework.controller

import com.demo.bigdata.spark_core.rdd.framework.common.TController
import com.demo.bigdata.spark_core.rdd.framework.service.WordCountService
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

class WordCountController extends TController{

  private val wordCountService = new WordCountService


  def dispatch()={

    val array:Array[(String,Int)] =wordCountService.dataAnalysis()
    array.foreach(println)

  }

}
