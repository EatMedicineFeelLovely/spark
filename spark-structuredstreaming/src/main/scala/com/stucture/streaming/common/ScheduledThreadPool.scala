package com.stucture.streaming.common

import java.util.concurrent.{Executor, Executors, ThreadFactory, TimeUnit}

import com.google.common.util.concurrent.ThreadFactoryBuilder

/**
  *用于定时更新参数
  * @param corePoolSize
  */
protected class ScheduledThreadPool(corePoolSize: Int = 1) {
  lazy val stp = Executors.newScheduledThreadPool(
    corePoolSize,
    new ThreadFactoryBuilder()
      .setDaemon(false)
      .setNameFormat("thread-pool-%d")
      .build)

  /**
    *
    * @param runnable
    * @param initialDelay
    * @param period
    * @param unit
    */
  def addScheduled(runnable: Runnable,
                   initialDelay: Long,
                   period: Long,
                   unit: TimeUnit): Unit = {
    stp.scheduleAtFixedRate(runnable, initialDelay, period, unit)
  }
}
object ScheduledThreadPool {
  def apply(corePoolSize: Int = 1): ScheduledThreadPool =
    new ScheduledThreadPool(corePoolSize)
}
