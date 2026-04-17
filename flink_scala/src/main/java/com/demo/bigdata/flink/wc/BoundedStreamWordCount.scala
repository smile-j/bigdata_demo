package com.demo.bigdata.flink.wc

import org.apache.flink.api.common.RuntimeExecutionMode
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

object BoundedStreamWordCount {

  def main(args: Array[String]): Unit = {

    /**
      * 流处理
      */

    //1.创建一个执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    /**
      * 流批一体的代码 通过流模式写
      * 实现批处理：1.通过代码设置；2. 命令行：bin/fink run -Dexecution.runtime-mode=BATCH
      */
    //env.setRuntimeMode(RuntimeExecutionMode.BATCH)
    //2.读取文件数据
    val lineData: DataStream[String] = env.readTextFile("datas/1.txt")

    val sum  = lineData.flatMap(_.split(" "))
      .map((_, 1))
//      .keyBy(0)
      .keyBy(_._1)
      .sum(1)
    sum.print()

    env.execute()
  }

}
