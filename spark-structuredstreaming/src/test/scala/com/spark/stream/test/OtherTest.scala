package com.spark.stream.test

import java.util.concurrent.{ConcurrentLinkedQueue, TimeUnit}

import com.stucture.streaming.common.ScheduledThreadPool

object OtherTest {
  def main(args: Array[String]): Unit = {
    val clq = new ConcurrentLinkedQueue[String]
    val nstp = ScheduledThreadPool()
//    nstp.addScheduled(() => run1(clq), 1L, 2L, TimeUnit.SECONDS)
//    nstp.addScheduled(() => run2(clq), 1L, 2L, TimeUnit.SECONDS)
//    nstp.addScheduled(() => run3(clq), 1L, 2L, TimeUnit.SECONDS)
//    nstp.addScheduled(() => run4(clq), 1L, 2L, TimeUnit.SECONDS)
  }
  def run1(clq: ConcurrentLinkedQueue[String]) {
    println("线程--- 1 ", Thread.currentThread().getName); clq.add("线程-- 1")
    // 线程里面的线程池
//    val sl = Executors.newFixedThreadPool(3)
//    sl.execute(() => {})
//    sl.execute(() => {})
//    sl.execute(() => {})
//    sl.shutdown
  }

  def run2(clq: ConcurrentLinkedQueue[String]) {
    println("线程--- 2 ", Thread.currentThread().getName); clq.add("线程-- 2")
  }

  def run3(clq: ConcurrentLinkedQueue[String]) {
    println("线程--- 3 ", Thread.currentThread().getName); clq.add("线程-- 3")
  }

  def run4(clq: ConcurrentLinkedQueue[String]) {
    println("线程--- 4 ", Thread.currentThread().getName)
    while (!clq.isEmpty) {
      clq.poll()
    }
    println("siez : ", clq.size)
  }
}
