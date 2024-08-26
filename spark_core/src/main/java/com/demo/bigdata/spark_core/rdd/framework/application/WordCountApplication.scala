package com.demo.bigdata.spark_core.rdd.framework.application

import com.demo.bigdata.spark_core.rdd.framework.common.TApplication
import com.demo.bigdata.spark_core.rdd.framework.controller.WordCountController

object WordCountApplication extends App with TApplication{

  start(appName = "worldCount"){
    val wordCountController = new WordCountController
    wordCountController.dispatch()
  }

}
