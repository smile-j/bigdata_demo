package com.demo.bigdata.spark_core.rdd.req

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spar02_HotCategoryTop10 {

  def main(args: Array[String]): Unit = {

    var sparkConf = new SparkConf().setMaster("local[*]").setAppName("HotCategoryTop10")
    val sc = new SparkContext(sparkConf)

    //q1:actionRdd重复使用
    //q2:cogroup性能不高

    //1.读取原始日志数据
    val actionRdd = sc.textFile("datas/user_visit_action.txt")
    actionRdd.cache()

    //2.统计品类的点击数量:(品类ID,点击数量)
    val clickActionRdd = actionRdd.filter(action => {
      val datas = action.split("_")
      datas(6) != "-1"
    })

    val clickDataRdd:RDD[(String,Int)] = clickActionRdd.map(action => {
      val datas = action.split("_")
      (datas(6), 1)
    })
    val clickCountRdd:RDD[(String,Int)] = clickDataRdd.reduceByKey(_ +_ )

    //3.统计品类的下单数量:(品类ID,下单数量)
    val orderActionRdd = actionRdd.filter(action => {
      val datas = action.split("_")
      datas(8) != "null"
    })
    val orderCountRdd:RDD[(String,Int)] = orderActionRdd.flatMap(action => {
      val datas = action.split("_")
      var cid = datas(8)
      val cids = cid.split(",")
      cids.map(id => (id, 1))
    }).reduceByKey(_ + _)



    //4.统计品类的支付数量:(品类ID,支付数量)
    val payActionRdd = actionRdd.filter(action => {
      val datas = action.split("_")
      datas(10) != "null"
    })
    val payCountRdd:RDD[(String,Int)] = payActionRdd.flatMap(action => {
      val datas = action.split("_")
      var cid = datas(10)
      val cids = cid.split(",")
      cids.map(id => (id, 1))
    }).reduceByKey(_ + _)

    //5.将品类进行排序,并且取前10名
    //  点击数量排序,下单数量排序,支付数量排序
    //  元组排序:先比较第一个,再比较第二个,再比较第三个,依次类推
    //  (品类id,(点击数量,下单数量,支付数量))

    val rdd1 = clickDataRdd.map({
      case (cid, cnt) => {
        (cid, (cnt, 0, 0))
      }
    })

    val rdd2 = orderCountRdd.map({
      case (cid, cnt) => {
        (cid, (0, cnt, 0))
      }
    })
    val rdd3 = payCountRdd.map({
      case (cid, cnt) => {
        (cid, (0, 0, cnt))
      }
    })
    //将3个数据源合并在一起,统一进行聚合计算
    val sourceRDD:RDD[(String,(Int,Int,Int))] = rdd1.union(rdd2).union(rdd3)

    val analysisRdd:RDD[(String,(Int,Int,Int))] = sourceRDD.reduceByKey(
      (t1, t2) => {
        (t1._1 + t2._1, t1._2 + t2._2, t1._3+ t2._3)
      }
    )


    val resultRdd = analysisRdd.sortBy(_._2,false).take(10)
    //6.结果打印
    resultRdd.foreach(println)

    sc.stop()

  }

}
