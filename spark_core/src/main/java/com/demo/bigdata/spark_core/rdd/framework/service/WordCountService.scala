package com.demo.bigdata.spark_core.rdd.framework.service

import com.demo.bigdata.spark_core.rdd.framework.common.TService
import com.demo.bigdata.spark_core.rdd.framework.dao.WordCountDao
import org.apache.spark.rdd.RDD

class WordCountService extends  TService{

  private val wordCountDao = new WordCountDao


  def dataAnalysis(): Array[(String,Int)] ={


    val lines: RDD[String] = wordCountDao.readFile("datas/1.txt")
    //2.将一行数据进行拆分
    val words:RDD[String] = lines.flatMap(_.split(" "))
    val worldToOne = words.map(wold=>(wold,1))
    val wordGroup:RDD[(String,Iterable[(String,Int)])] = worldToOne.groupBy(t=>t._1)


    val woldCount = wordGroup.map {
      case (world, list) => {
        list.reduce((t1, t2) => {
          (t1._1, t1._2 + t2._2)
        })
      }
    }
    val array:Array[(String,Int)] = woldCount.collect();
    array

  }

}
