package com.demo.bigdata.spark_core.rdd.dep

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark02_RDD_Dep {

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
    val lines:RDD[String] = sc.textFile("datas/1.txt");
    println(lines.dependencies)
    println("*"*20)
    //2.将一行数据进行拆分
    val words:RDD[String] = lines.flatMap(_.split(" "))
    println(words.dependencies)
    println("*"*20)
    val worldToOne = words.map(wold=>(wold,1))
    println(worldToOne.dependencies)
    println("*"*20)
    val woldCount =  worldToOne.reduceByKey(_+_)
    println(woldCount.dependencies)
    println("*"*20)

    val array:Array[(String,Int)] = woldCount.collect();
    array.sortBy(_._2)(Ordering.Int.reverse)

    array.foreach(println)
    //关闭连接
    sc.stop();
  }

}
