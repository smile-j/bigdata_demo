package com.demo.bigdata.spark_core.rdd.acc

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object Spark05_Bc {

  def main(args: Array[String]): Unit = {

    /**
      * 分布式广播变量
      *
      */

    var sparkConf = new SparkConf().setMaster("local").setAppName("wordCount")
    val sc = new SparkContext(sparkConf)

    val rdd1 = sc.makeRDD(List(
      ("a",1),("b",2),("c",3)
    ))

    val map = mutable.Map(
      ("a",2),("b",3),("c",4)
    )

    //封装广播变量
    val bc:Broadcast[mutable.Map[String,Int]] = sc.broadcast(map)


    rdd1.map({
      case (w,c)=>{
        //访问关播变量
        val newV = bc.value.getOrElse(w,0)
        (w,(c,newV))
      }
    }).collect().foreach(println)

    sc.stop()

  }

}
