package com.demo.bigdata.spark_sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, LongType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

object Spark03_SparkSql_UDAF {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("spark_sql")

    val spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

    val df:DataFrame = spark.read.json("datas/spark_sql/user.json")
    df.createOrReplaceTempView("user")

    //自定义聚合函数
    spark.udf.register("myAvg",new MyAvgUDAF())

    spark.sql("select myAvg(age) from user").show()

    spark.close()

  }


  /**
    * 自定义聚合函数
    *
    */
  class MyAvgUDAF extends UserDefinedAggregateFunction{

    //输入的数据的结构:in
    override def inputSchema: StructType = {
      StructType(Array(StructField("age",LongType)))
    }

    //缓冲区数据的结构:buffer
    override def bufferSchema: StructType = {
      StructType(Array(
        StructField("total",LongType),
        StructField("count",LongType)
      ))
    }

    //函数计算结果的数据结果：out
    override def dataType: DataType = LongType

    //函数的稳定性
    override def deterministic: Boolean = true

    //缓冲区的初始化
    override def initialize(buffer: MutableAggregationBuffer): Unit = {
//      buffer(0) = 0L
//      buffer(1) = 0L

      /**
        * 初始化值
        * StructField("total",LongType),
        * StructField("count",LongType)
        */
      buffer.update(0,0L)
      buffer.update(1,0L)

    }

    //根据输入值更新缓冲区数据
    override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
      buffer.update(0,buffer.getLong(0)+input.getLong(0))
      buffer.update(1,buffer.getLong(1)+1)
    }

    //缓冲区数据合并
    override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {

      buffer1.update(0,buffer1.getLong(0)+buffer2.getLong(0))
      buffer1.update(1,buffer1.getLong(1)+buffer2.getLong(1))

    }

    //计算平均值
    override def evaluate(buffer: Row): Any = {
      buffer.getLong(0)/buffer.getLong(1)
    }
  }

}
