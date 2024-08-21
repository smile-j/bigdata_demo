package com.demo.bigdata.spark_core.wc

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object WorldCount4 {

  def main(args: Array[String]): Unit = {

    //Application

    //Spark框架

    //建立和spark框架的连接
    val sparkConf = new SparkConf()
      .setMaster("local")
      .setAppName("WorldCount");
    val sc = new SparkContext(sparkConf);
    wordCount3(sc);

    //关闭连接
    sc.stop();

  }

  //groupBy
  def wordCount1(sc:SparkContext)={

    val rdd = sc.makeRDD(List("hello spark","hello scala"))
    val group = rdd.flatMap(_.split(" "))
      .groupBy(word => word)
    val worldCout = group.mapValues(iter=>iter.size)
    println(worldCout.collect().mkString(","))
  }

  //groupByKey
  def wordCount2(sc:SparkContext): Unit ={
    val rdd = sc.makeRDD(List("hello spark","hello scala"))
    val worldOne = rdd.flatMap(_.split(" "))
      .map((_,1))
    val worldCount = worldOne.groupByKey().mapValues(iter=>iter.size)
    println(worldCount.collect().mkString(","))

  }

  //reduceByKey
  def wordCount3(sc:SparkContext): Unit ={
    val rdd = sc.makeRDD(List("hello spark","hello scala"))
    val worldOne = rdd.flatMap(_.split(" "))
      .map((_,1))
    val worldCount = worldOne.reduceByKey(_+_)
    println(worldCount.collect().mkString(","))
  }

  //aggregateByKey
  def wordCount4(sc:SparkContext): Unit ={
    val rdd = sc.makeRDD(List("hello spark","hello scala"))
    val worldOne = rdd.flatMap(_.split(" "))
      .map((_,1))
    val worldCount = worldOne.aggregateByKey(0)(_+_,_+_)
  }

  //foldByKey
//  如果聚合计算时，分区内和分区间计算规则相同
  def wordCount5(sc:SparkContext): Unit ={
    val rdd = sc.makeRDD(List("hello spark","hello scala"))
    val worldOne = rdd.flatMap(_.split(" "))
      .map((_,1))
    val worldCount = worldOne.foldByKey(0)(_+_)
  }

  //combineByKey
  def wordCount6(sc:SparkContext): Unit ={
    val rdd = sc.makeRDD(List("hello spark","hello scala"))
    val worldOne = rdd.flatMap(_.split(" "))
      .map((_,1))
    val worldCount = worldOne.combineByKey(v=>v,(x:Int,y)=>x+y,(x:Int,y:Int)=>x+y)
  }

  //countByKey
  def wordCount7(sc:SparkContext): Unit ={
    val rdd = sc.makeRDD(List("hello spark","hello scala"))
    val worldOne = rdd.flatMap(_.split(" "))
      .map((_,1))
    val worldCount = worldOne.countByKey()
  }

  //countByValue
  def wordCount8(sc:SparkContext): Unit ={
    val rdd = sc.makeRDD(List("hello spark","hello scala"))
    val worlds = rdd.flatMap(_.split(" "))
    val worldCount = worlds.countByValue()
  }

  //reduce, aggregate ,fold
  def wordCount9(sc:SparkContext): Unit ={
    val rdd = sc.makeRDD(List("hello spark","hello scala"))
    val worlds = rdd.flatMap(_.split(" "))
    val mapworld = worlds.map(wold => {
      mutable.Map[String, Long]((wold, 1))
    })
    val worldMap = mapworld.reduce(
      (map1,map2)=>{
        map2.foreach{
          case (word,count)=>{
            val newValue= map1.getOrElse(word,0L)+count
            map1.update(word,newValue)
          }
        }
      map1
    })
  }
}
