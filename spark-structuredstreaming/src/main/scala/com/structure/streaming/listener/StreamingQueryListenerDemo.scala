package com.structure.streaming.listener

import com.alibaba.fastjson.JSON
import org.apache.spark.sql.streaming.{
  SourceProgress,
  StreamingQueryListener,
  StreamingQueryProgress
}

class StreamingQueryListenerDemo extends StreamingQueryListener {
  override def onQueryStarted(
      event: StreamingQueryListener.QueryStartedEvent
  ): Unit = {
    println(">>> onQueryStarted")
  }

  /**
    * stream sink输出完之后才调用？
    *
    * @param event
    */
  override def onQueryProgress(
      event: _root_.org.apache.spark.sql.streaming.StreamingQueryListener.QueryProgressEvent
  ): Unit = {
    val queryName = event.progress.name
    queryName match {
      case "aggregateQuery" =>
        println(event.progress.json) //getKafkaOffsetRange(event.progress.json)
      case "foreachBatchSinkQuery" => println(event.progress)
      case "StreamJoinQuery"       => getWatermark(event.progress)
    }
    //println("... onQueryProgress",event.progress.name)
    //event.progress.sources.foreach(x=>println(x.json))

  }

  /**
    *
    * @param progress
    */
  def getWatermark(progress: StreamingQueryProgress) = {
    val json = JSON.parseObject(progress.json)
    val eventTimeJ = json.getJSONObject("eventTime")
    println(eventTimeJ)
  }

  /**
    *
    * @param streamingQueryProgress
    */
  def getKafkaOffsetRange(streamingQueryProgress: StreamingQueryProgress) = {
    val json = streamingQueryProgress

  }

  /**
    *
    * @param event
    */
  override def onQueryTerminated(
      event: _root_.org.apache.spark.sql.streaming.StreamingQueryListener.QueryTerminatedEvent
  ): Unit = {
    println("### onQueryTerminated")

  }

}
