package com.demo.bigdata.spark_sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object Spark08_SparkSql_Test1 {

  def main(args: Array[String]): Unit = {

//    Class.forName("org.apache.spark.sql.hive.HiveSessionStateBuilder")

    //jdbc:hive2://hadoop102:10000
    //user:uhadoop
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("spark_sql")

    val spark: SparkSession = SparkSession.builder().enableHiveSupport().config(sparkConf).getOrCreate()

    spark.sql(
      """
        |select * from (
        |	select *,rank() over(partition by area order by clickCnt desc ) as rank
        |from(
        |	select area,product_name,count(1) clickCnt
        |	from (
        |		select a.*,p.product_name,c.city_name
        |		from user_visit_action a
        |		join product_info p on p.product_id=a.click_product_id
        |		join city_info c on c.city_id = a.city_id
        |		where a.click_product_id>-1
        |	)t1
        |	group by area,product_name
        |) t2
        |)t3 where t3.rank<=3
      """.stripMargin)


    spark.close()
  }

}
