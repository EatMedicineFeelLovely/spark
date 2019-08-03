package com.structure.streaming.sink

import org.apache.spark.sql.{ForeachWriter, Row}
import com.structure.streaming.caseclass.StructureStreamingCaseClass._

/**
  * 每个batch都会 new PrintlnSysSink 。 所以这里面的临时变量也只是在一个batch里面有效
  */
class PrintlnSysSink() extends ForeachWriter[AdlogData] {

  /**
    * 每个batch开始的时候调用
    *
    * @param partitionId
    * @param epochId
    * @return
    */
  override def open(partitionId: Long, epochId: Long): Boolean = {
    true
  }

  /**
    * @param value
    */
  override def process(value: AdlogData): Unit = {
    println(value)
  }

  /**
    * 每个 batch 结束的时候调用
    *
    * @param errorOrNull
    */
  override def close(errorOrNull: Throwable): Unit = {}
}
