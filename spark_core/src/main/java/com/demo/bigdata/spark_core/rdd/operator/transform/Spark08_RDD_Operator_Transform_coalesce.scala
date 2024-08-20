package com.demo.bigdata.spark_core.rdd.operator.transform

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD

object Spark08_RDD_Operator_Transform_coalesce {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
      .setMaster("local")
      .setAppName("WorldCount")
    import org.apache.spark.SparkContext
    val sc = new SparkContext(sparkConf)

    //todo 算子（方法、操作） coalesce
    //根据数据量缩减分区，用于大数据集过滤后，提高小数据集的执行效率
    //当spark程序中，存在过多的小任务的时候，可以通过coalesce方法，收缩/扩大 分区
    //减少或者扩大分区的个数，减小任务调度成本
    val rdd = sc.makeRDD(List(1,2,3,4,5,6),4)

    //默认情况下不会将分区的数据打乱重新组合
    //这种情况下的缩减分区可能会导致数据不均衡，出现数据倾斜
    //如果想要让数据均衡，可以进行shuffle处理

    //缩减分区：coalesce，如果想要数据均衡，可采用shuffle
    //扩大分区：repartition，底层代码coalesce(numPartitions, shuffle = true)
//    val newRDD:RDD[Int] = rdd.coalesce(3,true)
    val newRDD:RDD[Int] = rdd.repartition(3)
    newRDD.saveAsTextFile("outPath")



    //关闭连接
    sc.stop()
  }

}
