package com.demo.bigdata.spark_sql

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object Spark01_SparkSql_Basic {


  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("spark_sql")

    val spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

    //DateFrame
    val df:DataFrame = spark.read.json("datas/user.json")
    df.show( )
    //DateFrame=>SQL
//    df.createOrReplaceTempView("user")
//    spark.sql("select * from user ").show()
//    spark.sql("select id,name from user ").show()
//    spark.sql("select avg(age) from user ").show()

    //DateFrame=>DSL
//    df.select("age","name").show()
    //在使用DataFrame时，如果涉及到转换规则，需要引入转换规则
    //spark不是包名，上上下文对象
//    import spark.implicits._
//    df.select($"age"+1).show()

    //DataSet
    //DateFrame是其实就是特定泛型的DataSet
//    val seq = Seq(1,2,3,4)
//    val ds: Dataset[Int] = seq.toDS()
//    ds.show()
//    ds.createOrReplaceTempView("user_ids")
//    ds.select("value")

    //RDD<=> DateFrame
//    val rdd: RDD[(Int, String, Int)] = spark.sparkContext.makeRDD(List((1,"zhangsan",20),(2,"lisi",30)))
//    val df: DataFrame = rdd.toDF("id","name","age")
//    val rowRDD: RDD[Row] = df.rdd

    //DataSet<=> DateFrame
//    val ds: Dataset[User] = df.as[User]
//    val df1: DataFrame = ds.toDF()

    //RDD<=>DataSet
//    val ds1: Dataset[User] = rdd.map {
//      case (id, name, age) => {
//        User(id, name, age)
//      }
//    }.toDS()
//    val userRDD:RDD[User] = ds1.rdd

    spark.close()

  }

  case class User(id:Int,name:String,age:Int){

  }
}
