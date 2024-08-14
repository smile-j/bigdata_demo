package com.demo.bigdata.spark_core.rdd.builder

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark03_RDD_Memmory_Par {

  /**
    * 从内存创建RDD
    */

  def main(args: Array[String]): Unit = {

    //准备环境
    val conf = new SparkConf().setMaster("local[*]").setAppName("RDD")
      .set("spark.default.parallelism","3")
    val sc = new SparkContext(conf)

    //创建RDD
    //并行度 & 分区
    var seq = Seq[Int](1,2,3,4)
    //makeRDD 第二个参数 表示分区的数量
    //  可以不传递,那么makeRDD方法会使用默认值
    val rdd = sc.makeRDD(seq)

    //将处理的数据保存成分区文件
    rdd.saveAsTextFile("output")

    rdd.collect().foreach(e=>println("threadMain:"+Thread.currentThread().getName+",value:"+e))

    //关闭环境
    sc.stop()

  }

}
