package com.demo.bigdata.flink.wc

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

object UnBoundedStreamWordCount {

  def main(args: Array[String]): Unit = {

    /**
      * 无界
      * 模拟发送数据 nc -l -p 9999
      *  nc -lk 9999
      *
      * --host lochost --port 9999
      */
    //1.创建一个执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //2.读取文件数据
//    val lineData: DataStream[String] = env.socketTextStream("localhost",9999)
    val parameterTool = ParameterTool.fromArgs(args)
    val host = parameterTool.get("host")
    val port = parameterTool.getInt("port")
    printf(s"host:${host},port:${port}")
    val lineData: DataStream[String] = env.socketTextStream(host,port)

    val sum  = lineData.flatMap(_.split(" "))
      .map((_, 1))
//      .keyBy(0)
      .keyBy(_._1)
      .sum(1)
    sum.print()

    env.execute()
  }

}
