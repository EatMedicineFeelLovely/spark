package com.structure.streaming.func

import com.structure.streaming.caseclass.StructureStreamingCaseClass._
import org.apache.spark.sql.streaming.GroupState

object StructureStreamingWithStateFunc {

  /**
    * 一个batch调用一次
    *
    * @param sessionId
    * @param events
    * @param state
    * @return
    */
  def sessionAggMapWithState(sessionId: String,
                             events: Iterator[SessionLog],
                             state: GroupState[SessionStateInfo]) = {
    //如果超时了,状态的时间小于watermark的时间
    if (state.hasTimedOut) {
      val finalUpdate =
        SessionResult(
          sessionId,
          state.get.startTime,
          state.get.endTime,
          state.get.durationMs,
          state.get.numEvents,
          isOuttime = true
        ) //输出
      state.remove()
      finalUpdate
    } else {
      val timestamps = events.map(_.timeStamp).toSeq
      val updatedSession = if (state.exists) {
        val oldSession = state.get
        SessionStateInfo(
          oldSession.numEvents + timestamps.size,
          oldSession.startTime,
          math.max(oldSession.endTime, timestamps.max)
        )
      } else {
        SessionStateInfo(timestamps.size, timestamps.min, timestamps.max)
      }
      state.update(updatedSession)

      //val wartermarkT = state.getCurrentWatermarkMs() //这个watermark是所有数据最大的eventime -delay。
      //这个只能基于GroupStateTimeout.EventTimeTimeout才生效
      //只有当 当前sessionid 的 endTimestampMs < watermark的时候，这条数据才会失效
      state.setTimeoutTimestamp(updatedSession.endTime)
      //这个只能基于GroupStateTimeout.ProcessingTimeTimeout才生效
      //state.setTimeoutDuration("10 seconds")
      SessionResult(
        sessionId,
        state.get.startTime,
        state.get.endTime,
        state.get.durationMs,
        state.get.numEvents,
        isOuttime = false
      )
    }

  }

  /**
    * 在这里面可以做session的计算，对timestampList逐条做操作，可以输出多个结果
    *
    * @param sessionId
    * @param events
    * @param state
    * @return
    */
  def sessionAggFlatMapWithState(sessionId: String,
                                 events: Iterator[SessionLog],
                                 state: GroupState[SessionStateInfo]) = {
    //如果超时了
    if (state.hasTimedOut) {
      val finalUpdate =
        SessionResult(
          sessionId,
          state.get.startTime,
          state.get.endTime,
          state.get.durationMs,
          state.get.numEvents,
          isOuttime = true
        ) //输出
      state.remove()
      Iterator(finalUpdate)
    } else {
      val timestamps = events.map(_.timeStamp).toSeq
      val updatedSession = if (state.exists) {
        val oldSession = state.get
        SessionStateInfo(
          oldSession.numEvents + timestamps.size,
          oldSession.startTime,
          math.max(oldSession.endTime, timestamps.max)
        )
      } else {
        SessionStateInfo(timestamps.size, timestamps.min, timestamps.max)
      }
      state.update(updatedSession)

      val wartermarkT = state.getCurrentWatermarkMs() //这个watermark是所有数据最大的eventime -delay。
      //这个只能基于GroupStateTimeout.EventTimeTimeout才生效
      //只有当 当前sessionid 的 endTimestampMs < watermark的时候，这条数据才会失效
      state.setTimeoutTimestamp(updatedSession.endTime)
      //这个只能基于GroupStateTimeout.ProcessingTimeTimeout才生效
      //state.setTimeoutDuration("10 seconds")
      val re = SessionResult(
        sessionId,
        state.get.startTime,
        state.get.endTime,
        state.get.durationMs,
        state.get.numEvents,
        isOuttime = false
      )
      Iterator(re)
    }
  }
}
