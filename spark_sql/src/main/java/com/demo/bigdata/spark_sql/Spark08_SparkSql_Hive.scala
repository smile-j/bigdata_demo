package com.demo.bigdata.spark_sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}


object Spark08_SparkSql_Hive {

  def main(args: Array[String]): Unit = {

    Class.forName("org.codehaus.commons.compiler.InternalCompilerException")

    //jdbc:hive2://hadoop102:10000
    //user:uhadoop
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("spark_sql")

    val spark: SparkSession = SparkSession.builder().enableHiveSupport().config(sparkConf).getOrCreate()

    //使用sparkSQL连接外置的Hive
    //1.拷贝Hive-site.xml 文件到classPath
    //2.启动Hive的支持
    //3.增加对应的依赖关系
//    spark.sql("select * from test_user ").show()
    spark.sql("show tables").show()

    spark.close()
  }

}
