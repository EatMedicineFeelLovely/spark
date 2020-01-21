package com.spark.learn.udf
import org.apache.spark.sql.catalyst.expressions.aggregate.CollectList
import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{
  MutableAggregationBuffer,
  UserDefinedAggregateFunction
}
import org.apache.spark.sql.types.{
  ArrayType,
  DataType,
  IntegerType,
  StringType,
  StructField,
  StructType
}

class CollectToListUDFA extends UserDefinedAggregateFunction {
  var i = 0
  override def inputSchema: StructType =
    StructType(
      Array(StructField("input", IntegerType, true)))

  override def bufferSchema: StructType =
    StructType(Array(StructField("list", ArrayType(StringType), true)))

  override def dataType: DataType = ArrayType(StringType)

  override def deterministic: Boolean = true

  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = null
  }

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {

  }

  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1.size

  }

  override def evaluate(buffer: Row): Any = ???
}
