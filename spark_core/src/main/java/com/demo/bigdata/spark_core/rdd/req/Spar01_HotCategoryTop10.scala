package com.demo.bigdata.spark_core.rdd.req

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spar01_HotCategoryTop10 {

  def main(args: Array[String]): Unit = {

    var sparkConf = new SparkConf().setMaster("local[*]").setAppName("HotCategoryTop10")
    val sc = new SparkContext(sparkConf)

    //1.读取原始日志数据
    val actionRdd = sc.textFile("datas/user_visit_action.txt")

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
//    clickCountRdd.collect().foreach(println)
    println("*"*10)
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
//    orderCountRdd.collect().foreach(println)
    println("*"*20)


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
//    payCountRdd.collect().foreach(println)
    println("*"*30)
    //5.将品类进行排序,并且取前10名
    //  点击数量排序,下单数量排序,支付数量排序
    //  元组排序:先比较第一个,再比较第二个,再比较第三个,依次类推
    //  (品类id,(点击数量,下单数量,支付数量))
    //join zip leftOutjoin cogroup
    val cogroupRdd:RDD[(String,(Iterable[Int],Iterable[Int],Iterable[Int]))] = clickCountRdd.cogroup(orderCountRdd,payCountRdd)

//    cogroupRdd.collect().foreach(println)
    println("*"*40)
    val analysisRdd = cogroupRdd.mapValues({
      case (clickerIter, orderIter, payIter) => {

        var clickNum = 0
        val iterator1 = clickerIter.iterator
        if (iterator1.hasNext) {
          clickNum = iterator1.next()
        }

        var orderNum = 0
        val iterator2 = orderIter.iterator
        if (iterator2.hasNext) {
          orderNum = iterator2.next()
        }

        var payNum = 0
        val iterator3 = payIter.iterator
        if (iterator3.hasNext) {
          payNum = iterator3.next()
        }
        (clickNum,orderNum,payNum)
      }
    })
//      analysisRdd.collect().foreach(println)

    val resultRdd = analysisRdd.sortBy(_._2,false).take(10)
    //6.结果打印
    resultRdd.foreach(println)

    sc.stop()

  }

}
