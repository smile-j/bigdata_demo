package com.demo.bigdata.spark_sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

object Spark00_SparkSql_Basic {


  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("spark_sql")
    /**
      * 通用读
      * spark.read.load
      * spark.read.json
      *spark.read.format("json").load()
      *
      *
      * 通用写
      * df.write
      * df.write.format().save()
      * df.write.format("json").mode("append").save("output")
      *
      *
      * 读写默认格式 Parquet
      *   是一种能够有效存储嵌套数据的列示存储格式
      *  JSON
      *  CSV
      *  MySQL
      * Hive
      *
      */

    val spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

    val df:DataFrame = spark.read.json("datas/user.json")





    spark.close()

  }

  case class User(id:Int,name:String,age:Int){

  }
}
