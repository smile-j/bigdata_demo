package com.demo.bigdata.spark_core.rdd.operator.action

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD

object Spark06_RDD_Operator_foreach {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
      .setMaster("local")
      .setAppName("WorldCount")
    import org.apache.spark.SparkContext
    val sc = new SparkContext(sparkConf)

    /**
      * 行动算子
      */

    val rdd:RDD[Int] = sc.makeRDD(List(1,2,3,4),4)

    //foreach driver端内存集合的循环遍历方法
    rdd.collect().foreach(println)
    println("="*20)
    //foreach 其实是executor端内存数据打印
    rdd.foreach(println)
    println("="*20)
    var user = new User()

    //RDD算子中传递的函数是会包含闭包操作，那么就会进行监测功能
    //闭包检测
    rdd.foreach(int=>{
      println("age:"+(int+user.age))
    })


    //算子:Operator(操作)
    //     RDD的方法和Scala集合对象的方法不一样
    //      集合对象的方法都是在同一节点的内存中完成的
    //      RDD的方法可以将计算逻辑发送到Executor（分布式节点）端执行
    //      为了区分不同的处理效果，所以将RDD的方法称之为算子
    //      RDD的方法外的操作都是在Driver端执行的，而方法内部的逻辑代码都是在executor端执行的

    //关闭连接
    sc.stop()
  }


//  class User extends Serializable{
  //样例类，编译时会自动混入序列化特质（接口）
    case class User() {
    val age:Int=30

  }

}
