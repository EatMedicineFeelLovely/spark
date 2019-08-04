package com.spark.test

import org.apache.spark.scheduler.SparkListener
import org.apache.spark.scheduler.SparkListenerStageCompleted
import org.apache.spark.scheduler.SparkListenerApplicationStart

class SelfSparkListener extends SparkListener {
  override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit = {
    println(">>> onStageCompleted")
  }
  override def onApplicationStart(applicationStart: SparkListenerApplicationStart): Unit = {
    println(">>> onApplicationStart")

  }
  
}