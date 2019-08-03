package com.structure.streaming.func

import java.sql.Timestamp
import java.text.SimpleDateFormat

import com.structure.streaming.caseclass.StructureStreamingCaseClass._

import scala.collection.mutable.ListBuffer

object TransFormatFunc {

  /**
    *
    * @param itor
    * @return
    */
  def transToAdlog(itor: Iterator[String]): Iterator[AdlogData] = {
    val simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val re = new ListBuffer[AdlogData]
    itor.foreach { str =>
      val arr = str.split(",")
      val date = arr(0)
      val site = arr(1) //model uid
      //val site = arr(14) //model site= arr(8)
      val timeStamp = simpleDateFormat.parse(date).getTime
      val eventTime = new Timestamp(timeStamp)
      re.+=(AdlogData(date, site, eventTime, timeStamp))
    }
    re.toIterator
  }

  /**
    *
    * @param itor
    */
  def transToAdClickLog(itor: Iterator[String]) = {
    val simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val re = new ListBuffer[AdClickLog]
    itor.foreach { str =>
      val arr = str.split(",")
      val date = arr(0)
      val uid = arr(3) //model uid
      val timeStamp = simpleDateFormat.parse(date).getTime
      val eventTime = new Timestamp(timeStamp)
      re.+=(AdClickLog(date, uid, eventTime, timeStamp))
    }
    re.toIterator
  }

  /**
    *
    * @param itor
    */
  def transToAdImpressionLog(itor: Iterator[String]) = {
    val simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val re = new ListBuffer[AdImpressionLog]
    itor.foreach { str =>
      val arr = str.split(",")
      val date = arr(0)
      val uid = arr(1) //model uid
      val timeStamp = simpleDateFormat.parse(date).getTime
      val eventTime = new Timestamp(timeStamp)
      re.+=(AdImpressionLog(date, uid, eventTime, timeStamp))
    }
    re.toIterator
  }

  /**
    *
    * @param itor
    * @return
    */
  def transToSessionLog(itor: Iterator[String]) = {
    val simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val re = new ListBuffer[SessionLog]
    itor.foreach { str =>
      val arr = str.split(",")
      val dateStr = arr(0)
      val sessionid = arr(14)
      val timeStamp = simpleDateFormat.parse(dateStr).getTime
      val eventTime = new Timestamp(timeStamp)
      re.+=(SessionLog(dateStr, sessionid, eventTime, timeStamp))
    }
    re.toIterator
  }

  /**
    *
    * @param itor
    */
  def transCsvToSessionLog(itor: Iterator[String]) = {
    val simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val re = new ListBuffer[SessionLog]
    itor.foreach { str =>
      val arr = str.split(",")
      val sessionid = arr(0)
      val date = arr(1)
      val timeStamp = simpleDateFormat.parse(date).getTime
      val eventTime = new Timestamp(timeStamp)
      re.+=(SessionLog(date, sessionid, eventTime, timeStamp))
    }
    re.toIterator
  }
}
