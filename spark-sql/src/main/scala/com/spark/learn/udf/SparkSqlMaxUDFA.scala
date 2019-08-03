package com.spark.learn.udf

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{
  MutableAggregationBuffer,
  UserDefinedAggregateFunction
}
import org.apache.spark.sql.types.{
  DataType,
  IntegerType,
  StringType,
  StructField,
  StructType
}

class SparkSqlMaxUDFA extends UserDefinedAggregateFunction {

  override def inputSchema: StructType =
    StructType(Array(StructField("input", IntegerType, true)))

  /**
    * buffer里面的类型
    * @return
    */
  override def bufferSchema: StructType =
    StructType(Array(StructField("count", IntegerType, true)))

  /**
    * 最终输出类型
    * @return
    */
  override def dataType: DataType = IntegerType

  override def deterministic: Boolean = true

  /**
    * 初始化话第一个buffer的值
    * @param buffer
    */
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = 0
  } //初始默认值
  /**
    * 单个partition的时候会调用
    * @param buffer
    * @param input
    */
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    val last = buffer.getAs[Int](0)
    val next = input.getAs[Int](0)
    if (next > last) buffer(0) = next
  }

  /**
    * shuffle merge，在shuffle的时候会调用
    * @param buffer1
    * @param buffer2
    */
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    if (buffer1.getAs[Int](0) < buffer2.getAs[Int](0)) {
      buffer1(0) = buffer2(0)
    }

  }

  override def evaluate(buffer: Row): Any = buffer.getAs[Int](0)
}
