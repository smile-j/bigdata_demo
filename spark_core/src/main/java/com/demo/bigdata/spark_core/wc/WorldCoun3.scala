package com.demo.bigdata.spark_core.wc

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object WorldCoun3{

  def main(args: Array[String]): Unit = {

    //Application

    //Spark框架

    //建立和spark框架的连接
    val sparkConf = new SparkConf()
      .setMaster("local")
      .setAppName("WorldCount");
    val sc = new SparkContext(sparkConf);
    //执行业务操作
    //1.读取文件，获取一行一行的数据  hello world
    val lines:RDD[String] = sc.textFile("datas");
    //2.将一行数据进行拆分
    val words:RDD[String] = lines.flatMap(_.split(" "))
    val worldToOne = words.map(wold=>(wold,1))

    val woldCount =  worldToOne.reduceByKey(_+_)

//    val wordGroup:RDD[(String,Iterable[(String,Int)])] = worldToOne.groupBy(t=>t._1)
//
//
//    val woldCount = wordGroup.map {
//      case (world, list) => {
//        list.reduce((t1, t2) => {
//          (t1._1, t1._2 + t2._2)
//        })
//      }
//    }
//
    val array:Array[(String,Int)] = woldCount.collect();
    array.sortBy(_._2)(Ordering.Int.reverse)

    array.foreach(println)
    //关闭连接
    sc.stop();
  }

}
