package com.demo.bigdata.spark_core.wc

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object WorldCount {

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
    val lines:RDD[String] = sc.textFile("datas");
    //2.将一行数据进行拆分
    val words:RDD[String] = lines.flatMap(_.split(" "))

    //3.将数据根据单词进行分组统计
    val wordGroup:RDD[(String,Iterable[String])] = words.groupBy(word => word);

    //4.对分组的数据进行转换
    val worldToCount = wordGroup.map {
      case (word, list) => {
        (word, list.size)
      }
    }

    val array:Array[(String,Int)] = worldToCount.collect();

    array.foreach(println)

    //关闭连接
    sc.stop();

  }

}
