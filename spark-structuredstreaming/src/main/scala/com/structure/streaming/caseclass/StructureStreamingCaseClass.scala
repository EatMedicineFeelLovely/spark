package com.structure.streaming.caseclass

import java.sql.Timestamp

object StructureStreamingCaseClass {

  /**
    * adlog
    *
    * @param date
    * @param site
    * @param eventTime
    * @param timeStamp
    */
  case class AdlogData(date: String,
                       site: String,
                       eventTime: Timestamp,
                       timeStamp: Long)

  /**
    *
    * @param date
    * @param clickUid
    * @param clickEventTime
    * @param clickTimeStamp
    */
  case class AdClickLog(clickdate: String,
                        clickUid: String,
                        clickEventTime: Timestamp,
                        clickTimeStamp: Long)

  /**
    *
    * @param date
    * @param impressUid
    * @param impressEventTime
    * @param impressTimeStamp
    */
  case class AdImpressionLog(impressdate: String,
                             impressUid: String,
                             impressEventTime: Timestamp,
                             impressTimeStamp: Long)

  /**
    *session log
    *
    * @param dateStr
    * @param sessionid
    * @param eventTime
    * @param timeStamp
    */
  case class SessionLog(dateStr: String,
                        sessionid: String,
                        eventTime: Timestamp,
                        timeStamp: Long)

  /**
    * session的状态info
    *
    * @param numEvents
    * @param startTime
    * @param endTime
    */
  case class SessionStateInfo(numEvents: Int, startTime: Long, endTime: Long) {
    val durationMs = endTime - startTime
  }

  /**
    * session的计算结果
    *
    * @param sessionId
    * @param startTime
    * @param endTime
    * @param durationMs
    * @param numEvents
    * @param isOuttime
    */
  case class SessionResult(sessionId: String,
                           startTime: Long,
                           endTime: Long,
                           durationMs: Long,
                           numEvents: Int,
                           isOuttime: Boolean)
}
