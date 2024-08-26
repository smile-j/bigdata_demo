package com.demo.bigdata.spark_core.rdd.acc

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object Spark04_Bc {

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



    //会导致数据几何增长,并且影响shuffle的性能,不推荐使用
//    val joinRdd = rdd1.join(rdd2)
//    joinRdd.collect().foreach(println)

    rdd1.map({
      case (w,c)=>{
        val newV = map.getOrElse(w,0)
        (w,(c,newV))
      }
    }).collect().foreach(println)

    sc.stop()

  }

}
