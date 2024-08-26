package com.demo.bigdata.spark_core.rdd.part

import org.apache.spark.rdd.RDD
import org.apache.spark.{Partitioner, SparkConf, SparkContext}

object Spark01_RDD_Park {

  /**
    * 分区
    */

  def main(args: Array[String]): Unit = {
    var sparkConf = new SparkConf().setMaster("local").setAppName("wordCount")
    val sc = new SparkContext(sparkConf)

    val rdd:RDD[(String,String)] = sc.makeRDD(List(
      ("nba", "xxxxxxxx"),
      ("cba", "xxxxxxxx"),
      ("wnba", "xxxxxxxx"),
      ("nba", "xxxxxxxx")
    ), 3)
    val partRdd:RDD[(String,String)] = rdd.partitionBy(new myPartitioner)

    partRdd.saveAsTextFile("output")


    sc.stop()
  }

  /**
    * 自定义分区
    */
  class myPartitioner extends Partitioner{

    override def numPartitions: Int = 3

    //根据数据的key值,返回数据的分区索引,从0开始
    override def getPartition(key: Any): Int = {

      key match {
        case "nba" => 0
        case "wnba" => 1
        case _ => 2
      }

    }

  }


}
