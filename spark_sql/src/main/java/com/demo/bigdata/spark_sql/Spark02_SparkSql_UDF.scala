package com.demo.bigdata.spark_sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

object Spark02_SparkSql_UDF {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("spark_sql")

    val spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

    val df:DataFrame = spark.read.json("datas/user.json")
    df.createOrReplaceTempView("user")

    //自定义函数
    spark.udf.register("prefixName",(name)=>{
      "userName:"+name
    })

    spark.sql("select age,prefixName(name) from user").show()

    spark.close()

  }

}
