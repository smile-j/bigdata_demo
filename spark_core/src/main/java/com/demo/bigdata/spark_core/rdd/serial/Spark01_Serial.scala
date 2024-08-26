package com.demo.bigdata.spark_core.rdd.serial

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark01_Serial {

  /**
    * 俩种序列化方式
    * Kryo
    * Serializable
    *
    *
    */

  def main(args: Array[String]): Unit = {
    import org.apache.spark.SparkConf
    import org.apache.spark.SparkContext
    val sparkConf = new SparkConf().setMaster("local").setAppName("WorldCount")
    val sc = new SparkContext(sparkConf)

    val rdd = sc.makeRDD(Array("hello world","hello spark","hive","scala"))
    val search = new Search("h")

    search.getMatch1(rdd )
    search.getMatch2(rdd )


    sc.stop()
  }

  class Search(query:String) extends Serializable{

    def isMatch(s:String):Boolean={
        s.contains(query)
    }

    def getMatch1(rdd:RDD[String])={
        rdd.filter(isMatch)
    }

    def getMatch2(rdd:RDD[String])={
        rdd.filter(x=>x.contains(query))
    }

  }

}
