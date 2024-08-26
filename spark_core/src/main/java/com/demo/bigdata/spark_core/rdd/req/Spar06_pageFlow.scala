package com.demo.bigdata.spark_core.rdd.req

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spar06_pageFlow {

  def main(args: Array[String]): Unit = {

    /**
      * 页面跳转率
      */
    var sparkConf = new SparkConf().setMaster("local[*]").setAppName("HotCategoryTop10")
    val sc = new SparkContext(sparkConf)

    //1.读取原始日志数据
    val actionRdd = sc.textFile("datas/user_visit_action.txt")

    val dataActionRDD = actionRdd.map(
      action => {
        val datas = action.split("_")
        UserVisitAction(
          datas(0),
          datas(1).toLong,
          datas(2),
          datas(3).toLong,
          datas(4),
          datas(5),
          datas(6).toLong,
          datas(7).toLong,
          datas(8),
          datas(9),
          datas(10),
          datas(11),
          datas(12).toLong,
        )
      })

    dataActionRDD.cache()

    //todo 指定页面的调整统计
    var ids = List(1L,2L,3L,4L,5L,6L,7L)
    println("==================ids:"+ids.mkString(","))
    println("==================tail:"+ids.tail.mkString(","))
    var okFlowIds: List[(Long, Long)] = ids.zip(ids.tail)

    //计算分母
    val pageMapRdd:RDD[(Long,Int)] = dataActionRDD
        .filter(
          action=>{
            ids.init.contains(action.page_id)
        }).map(
      action => {
        (action.page_id, 1)
      }
    )
    val pageIdToCountMap:Map[Long,Int] = pageMapRdd.reduceByKey(_ + _).collect().toMap

    //计算分子
    val sessionRdd:RDD[(String,Iterable[UserVisitAction])] = dataActionRDD.groupBy(_.session_id)

    val mvRDD: RDD[(String, List[((Long, Long), Int)])] = sessionRdd.mapValues(
      iter => {
        val sortList = iter.toList.sortBy(_.action_time)
        val flowIds: List[Long] = sortList.map(_.page_id)

        val pageFlowIds: List[(Long, Long)] = flowIds.zip(flowIds.tail)
        pageFlowIds
          //不符合的过滤
          .filter(t=>{
          okFlowIds.contains(t)
        })
          .map(t=>(t,1))
      }
    )
    //((1,2),1)
    val flatRdd: RDD[((Long, Long), Int)] = mvRDD.map(_._2).flatMap(list=>list)

    val dataRDD: RDD[((Long, Long), Int)] = flatRdd.reduceByKey(_+_)

    //计算单挑跳转率
    dataRDD.foreach{
      case ((pageId1,pageId2),sum) =>{
        val long = pageIdToCountMap.getOrElse(pageId1,0)
        println(s"页面单挑：${pageId1} ==> ${pageId2},转换率:"+(sum.toDouble/long))
      }
    }




    sc.stop()

  }

  case class UserVisitAction(
                            date:String,//用户点击行为时间
                            user_id:Long,//用户的ID
                            session_id:String,//Session_id
                            page_id:Long,//页面id
                            action_time:String,//动作的时间点
                            search_keyword:String,//用户搜素的关键词
                            click_category_id:Long,//某一个商品品类的id
                            click_product_id:Long,//某一个商品的ID
                            order_category_ids:String,//一次订单中所有品类的id集合
                            order_produce_ids:String,//一次订单中所有品类的id集合
                            pay_category__ids:String,//一次支付所有品类的ID集合
                            pay_product_ids:String,//一次支付中所有商品的ID集合
                            city_id:Long //城市id
                            ){





  }

}
