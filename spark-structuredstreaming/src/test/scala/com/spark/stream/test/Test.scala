package com.spark.stream.test

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.concurrent.CountDownLatch

import com.google.common.cache.CacheLoader

object Test {
  val terminationLatch = new CountDownLatch(1)
  val s = new Thread(new Runnable {
    override def run(): Unit = {
      terminationLatch.countDown()
      Thread.sleep(5000)
      println("thread ... start")
      // Thread.sleep(2000)
      println("thread ... end")
    }
  })
  def main(args: Array[String]): Unit = {
    s.start()
    terminationLatch.await()
    println(">>>> end ")

  }
}
