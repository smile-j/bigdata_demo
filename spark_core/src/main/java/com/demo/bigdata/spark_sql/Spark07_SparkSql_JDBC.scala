package com.demo.bigdata.spark_sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object Spark07_SparkSql_JDBC {

  def main(args: Array[String]): Unit = {

//    System.setProperties("UHADOOP_USER_NAME","uhadoop")
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("spark_sql")

    val spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    import spark.implicits._

    val df: DataFrame = spark.read
      .format("jdbc")
      .option("url", "jdbc:mysql://localhost:3306/test")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("user", "root")
      .option("password", "root")
      .option("dbtable", "test1")
      .load()
    df.show()

    //保存数据
//    df.write
//      .format("jdbc")
//      .option("url", "jdbc:mysql://localhost:3306/test")
//      .option("driver", "com.mysql.jdbc.Driver")
//      .option("user", "root")
//      .option("password", "root")
//      .option("dbtable", "test1_bak")
//      .mode(SaveMode.Append)
//      .save()
//    df.write
//      .format("csv")
//      .save("output")

    spark.close()
  }

}
