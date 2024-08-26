package com.demo.bigdata.spark_core.rdd.framework.common

import com.demo.bigdata.spark_core.rdd.framework.util.EnvUtil
import org.apache.spark.rdd.RDD

trait TDao {

  def readFile(path:String): RDD[String] ={
    //执行业务操作
    //1.读取文件，获取一行一行的数据  hello world
    val lines:RDD[String] = EnvUtil.get().textFile(path);
    lines
  }

}
