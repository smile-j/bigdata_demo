package com.demo.bigdata.spark_core.rdd.req

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spar03_HotCategoryTop10 {

  def main(args: Array[String]): Unit = {

    var sparkConf = new SparkConf().setMaster("local[*]").setAppName("HotCategoryTop10")
    val sc = new SparkContext(sparkConf)

    //q1:大量的shuffle(reduceByKey)
    //reduceByKey 聚合算子，spark会提供优化，缓存

    //1.读取原始日志数据
    val actionRdd = sc.textFile("datas/user_visit_action.txt")
    actionRdd.cache()

    val flagRdd:RDD[(String,(Int,Int,Int))] = actionRdd.flatMap(action => {
      val datas:Array[String] = action.split("_")
      //点击场景
      if (datas(6) != "-1") {
        List((datas(6), (1, 0, 0)))
        //下单
      }else if (datas(8) != "null") {
        val ids = datas(8).split(",")
        ids.map(id => (id, (0, 1, 0)))
      } else if (datas(10) != "null") {
        val ids = datas(10).split(",")
        ids.map(id => (id, (0, 0, 1)))
      } else {
        Nil
      }

    })
    val analysisRdd = flagRdd.reduceByKey(
      (t1, t2) => {
        (t1._1 + t2._1, t1._2 + t2._2, t1._3 + t2._3)
      }
    )

    val resultRdd = analysisRdd.sortBy(_._2,false).take(10)
    //6.结果打印
    resultRdd.foreach(println)

    sc.stop()

  }

}
