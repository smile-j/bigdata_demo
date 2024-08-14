package com.demo.bigdata.spark_core.test

import java.io.ObjectOutputStream
import java.net.Socket

object Driver {

  def main(args: Array[String]): Unit = {

    val client1 = new Socket("localhost",9999)
    val client2 = new Socket("localhost",8888)
    val outputStream1 = client1.getOutputStream

    val objectOutputStream1 = new ObjectOutputStream(outputStream1)
    val task = new Task()

    val subTask = new SubTask()
    subTask.logic = task.logic
    subTask.data = task.data.take(2)

    objectOutputStream1.writeObject(subTask)
//    outputStream.write(2)
    objectOutputStream1.flush()
    objectOutputStream1.close()
    objectOutputStream1.close()

  //-----------------------------------------
    val outputStream2 = client2.getOutputStream

    val objectOutputStream2 = new ObjectOutputStream(outputStream2)

    val subTask2 = new SubTask()
    subTask2.logic = task.logic
    subTask2.data = task.data.takeRight(2)

    objectOutputStream2.writeObject(subTask2)
    //    outputStream.write(2)
    objectOutputStream2.flush()
    objectOutputStream2.close()
    objectOutputStream2.close()

    println("发送完毕")

  }

}
