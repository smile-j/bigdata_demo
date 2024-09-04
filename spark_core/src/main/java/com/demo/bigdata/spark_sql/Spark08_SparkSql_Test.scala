package com.demo.bigdata.spark_sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object Spark08_SparkSql_Test {

  def main(args: Array[String]): Unit = {

//    Class.forName("org.apache.spark.sql.hive.HiveSessionStateBuilder")

    //jdbc:hive2://hadoop102:10000
    //user:uhadoop
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("spark_sql")

    val spark: SparkSession = SparkSession.builder().enableHiveSupport().config(sparkConf).getOrCreate()

    spark.sql("use test_demo")
    //user_visit_action
    spark.sql(
      """
        |CREATE TABLE `user_visit_action`(
        |  `date` string,
        |  `user_id` bigint,
        |  `session_id` string,
        |  `page_id` bigint,
        |  `action_time` string,
        |  `search_keyword` string,
        |  `click_category_id` bigint,
        |  `click_product_id` bigint,
        |  `order_category_ids` string,
        |  `order_product_ids` string,
        |  `pay_category_ids` string,
        |  `pay_product_ids` string,
        |  `city_id` bigint)
        |row format delimited fields terminated by '\t';
      """.stripMargin)

    spark.sql(
      """
        |load data local inpath 'datas/spark_sql/user_visit_action.txt' into table test_demo.user_visit_action;
      """.stripMargin)

    //product_info
    spark.sql(
      """
        |CREATE TABLE `product_info`(
        |  `product_id` bigint,
        |  `product_name` string,
        |  `extend_info` string)
        |row format delimited fields terminated by '\t';
      """.stripMargin)
    spark.sql(
      """
        |load data local inpath 'input/product_info.txt' into table test_demo.product_info
      """.stripMargin)

    //city_info
    spark.sql(
      """
        |CREATE TABLE `city_info`(
        |  `city_id` bigint,
        |  `city_name` string,
        |  `area` string)
        |row format delimited fields terminated by '\t';
      """.stripMargin)

    spark.sql(
      """
        |load data local inpath 'input/city_info.txt' into table test_demo.city_info
      """.stripMargin)

    spark.close()
  }

}
