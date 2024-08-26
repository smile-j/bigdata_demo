package com.demo.bigdata.spark_core.rdd.req

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spar05_HotCategoryTop10_Session {

  def main(args: Array[String]): Unit = {

    /**
      * 统计top10,热门品类中每个品类的top10活跃session统计
      *  统计每个品类用户的session的点击统计
       */

    var sparkConf = new SparkConf().setMaster("local[*]").setAppName("HotCategoryTop10")
    val sc = new SparkContext(sparkConf)

    val actionRdd:RDD[String] = sc.textFile("datas/user_visit_action.txt")

    actionRdd.cache()

    val top10Ids:Array[String] = top10(actionRdd).map(_._1)

    //1.过滤原始数据,保留留点击和前10品牌ID
    val filterActionAdd = actionRdd.filter(
      action => {
        val datas = action.split("_")
        if (datas(6) != "-1") {
          top10Ids.contains(datas(6))
        } else {
          false
        }
      }
    )

    //2.根据品类ID和sessionid进行点击量的统计
    val reduceRDD:RDD[((String,String),Int)] = filterActionAdd.map(
      action => {
        val datas = action.split("_")
        ((datas(6), datas(2)), 1)
      }
    ).reduceByKey(_ + _)
    //3.将统计的结果进行结构的转换
    //（（品类id,sessionId）,sum）=>(品牌id,(sessionId ,sum))
    val mapRDD:RDD[(String,(String,Int))] = reduceRDD.map {
      case ((cid, sid), num) => {
        (cid, (sid, num))
      }
    }
    //4.相同的品类进行分组
    val groupRDD:RDD[(String,Iterable[(String,Int)])] = mapRDD.groupByKey()

    //5.分组后的数据进行点击量的排序，取前10名
    val result = groupRDD.mapValues(
      iter => {
        iter.toList.sortBy(_._2)(Ordering.Int.reverse).take(10)
      }
    )
    result.collect().foreach(println)

    sc.stop()

  }

  def top10(actionRdd:RDD[String])={

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
    resultRdd
  }

}
