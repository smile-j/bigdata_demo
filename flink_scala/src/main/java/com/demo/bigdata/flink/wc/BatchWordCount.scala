package com.demo.bigdata.flink.wc


import org.apache.flink.api.scala._
object BatchWordCount {

  def main(args: Array[String]): Unit = {

    /**
      * 批处理不推荐使用
      * 推荐使用流处理，在提交任务通讯时通过将执行模式设为BATCH来进行批处理
      * bin/flink run -Dexecution.runtime-mode=BATCH BatchWordCount.jar
      */

    //1.创建一个执行环境
    val env = ExecutionEnvironment.getExecutionEnvironment
    //2.读取文件数据
    val lineDataSet: DataSet[String] = env.readTextFile("datas/1.txt")
    //3.对数据集进行转换处理
    val wordAndOne: DataSet[(String, Int)] = lineDataSet
      .flatMap(_.split(" "))
      .map((_, 1))
    //4.分组
    val wordAndOneGroup = wordAndOne.groupBy(0)

    //5.聚合
    val sum: AggregateDataSet[(String, Int)] = wordAndOneGroup.sum(1)

    sum.print()
  }

}
