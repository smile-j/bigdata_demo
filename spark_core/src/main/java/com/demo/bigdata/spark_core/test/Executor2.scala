package com.demo.bigdata.spark_core.test

import java.io.ObjectInputStream
import java.net.ServerSocket

object Executor2 {

  def main(args: Array[String]): Unit = {
    val server:ServerSocket = new ServerSocket(8888)
    println("8888...服务启动，等待接受数据")
    val client = server.accept()

    val inputStream = client.getInputStream
    val objectInputStream = new ObjectInputStream(inputStream)
    val task = objectInputStream.readObject().asInstanceOf[SubTask]

    val res = task.compute()

    println("接受到客户端的数据："+res)
    objectInputStream.close()
    client.close()
    server.close()



  }

}
